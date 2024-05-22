use std::env;
use std::io::{stderr, LineWriter, Write};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use actix_web::http::KeepAlive;
use actix_web::web::Data;
use actix_web::HttpServer;
use index_scheduler::IndexScheduler;
use is_terminal::IsTerminal;
use search_server::analytics::Analytics;
use search_server::option::LogMode;
use search_server::{
    analytics, create_app, setup_search_server, LogRouteHandle, LogRouteType, LogStderrHandle,
    LogStderrType, Opt, SubscriberForSecondLayer,
};
use auth::{generate_master_key, AuthController, MASTER_KEY_MIN_SIZE};
use mimalloc::MiMalloc;
use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::Layer;

#[global_allocator]
static ALLOC: MiMalloc = MiMalloc;

fn default_log_route_layer() -> LogRouteType {
    None.with_filter(tracing_subscriber::filter::Targets::new().with_target("", LevelFilter::OFF))
}

fn default_log_stderr_layer(opt: &Opt) -> LogStderrType {
    let layer = tracing_subscriber::fmt::layer()
        .with_writer(|| LineWriter::new(std::io::stderr()))
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE);

    let layer = match opt.experimental_logs_mode {
        LogMode::Human => Box::new(layer)
            as Box<dyn tracing_subscriber::Layer<SubscriberForSecondLayer> + Send + Sync>,
        LogMode::Json => Box::new(layer.json())
            as Box<dyn tracing_subscriber::Layer<SubscriberForSecondLayer> + Send + Sync>,
    };

    layer.with_filter(
        tracing_subscriber::filter::Targets::new()
            .with_target("", LevelFilter::from_str(&opt.log_level.to_string()).unwrap()),
    )
}

/// does all the setup before search_server is launched
fn setup(opt: &Opt) -> anyhow::Result<(LogRouteHandle, LogStderrHandle)> {
    let (route_layer, route_layer_handle) =
        tracing_subscriber::reload::Layer::new(default_log_route_layer());
    let route_layer: tracing_subscriber::reload::Layer<_, _> = route_layer;

    let (stderr_layer, stderr_layer_handle) =
        tracing_subscriber::reload::Layer::new(default_log_stderr_layer(opt));
    let route_layer: tracing_subscriber::reload::Layer<_, _> = route_layer;

    let subscriber = tracing_subscriber::registry().with(route_layer).with(stderr_layer);

    // set the subscriber as the default for the application
    tracing::subscriber::set_global_default(subscriber).unwrap();

    Ok((route_layer_handle, stderr_layer_handle))
}

fn on_panic(info: &std::panic::PanicInfo) {
    let info = info.to_string().replace('\n', " ");
    tracing::error!(%info);
}

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    let (opt, config_read_from) = Opt::try_build()?;

    std::panic::set_hook(Box::new(on_panic));



    let log_handle = setup(&opt)?;



    let (index_scheduler, auth_controller) = setup_search_server(&opt)?;

    #[cfg(all(not(debug_assertions), feature = "analytics"))]
    let analytics = if !opt.no_analytics {
        analytics::SegmentAnalytics::new(&opt, index_scheduler.clone(), auth_controller.clone())
            .await
    } else {
        analytics::MockAnalytics::new(&opt)
    };
    #[cfg(any(debug_assertions, not(feature = "analytics")))]
    let analytics = analytics::MockAnalytics::new(&opt);

    print_launch_resume(&opt, analytics.clone(), config_read_from);

    run_http(index_scheduler, auth_controller, opt, log_handle, analytics).await?;

    Ok(())
}

async fn run_http(
    index_scheduler: Arc<IndexScheduler>,
    auth_controller: Arc<AuthController>,
    opt: Opt,
    logs: (LogRouteHandle, LogStderrHandle),
    analytics: Arc<dyn Analytics>,
) -> anyhow::Result<()> {
    let enable_dashboard = &opt.env == "development";
    let opt_clone = opt.clone();
    let index_scheduler = Data::from(index_scheduler);
    let auth_controller = Data::from(auth_controller);

    let http_server = HttpServer::new(move || {
        create_app(
            index_scheduler.clone(),
            auth_controller.clone(),
            opt.clone(),
            logs.clone(),
            analytics.clone(),
            enable_dashboard,
        )
    })
    // Disable signals allows the server to terminate immediately when a user enter CTRL-C
    .disable_signals()
    .keep_alive(KeepAlive::Os);

    if let Some(config) = opt_clone.get_ssl_config()? {
        http_server.bind_rustls_021(opt_clone.http_addr, config)?.run().await?;
    } else {
        http_server.bind(&opt_clone.http_addr)?.run().await?;
    }
    Ok(())
}

pub fn print_launch_resume(
    opt: &Opt,
    analytics: Arc<dyn Analytics>,
    config_read_from: Option<PathBuf>,
) {

    let protocol =
        if opt.ssl_cert_path.is_some() && opt.ssl_key_path.is_some() { "https" } else { "http" };

    eprintln!(
        "Config file path:\t{:?}",
        config_read_from
            .map(|config_file_path| config_file_path.display().to_string())
            .unwrap_or_else(|| "none".to_string())
    );
    eprintln!("Database path:\t\t{:?}", opt.db_path);
    eprintln!("Server listening on:\t\"{}://{}\"", protocol, opt.http_addr);
    // eprintln!("Environment:\t\t{:?}", opt.env);
    // eprintln!("Version:\t{:?}", env!("CARGO_PKG_VERSION").to_string());

    if let Some(instance_uid) = analytics.instance_uid() {
        eprintln!("Instance UID:\t\t\"{}\"", instance_uid);
    }

    eprintln!();

    // match (opt.env.as_ref(), &opt.master_key) {
    //     ("production", Some(_)) => {
    //         eprintln!("A master key has been set. Requests to Search Server won't be authorized unless you provide an authentication key.");
    //     }
    //     ("development", Some(master_key)) => {
    //         eprintln!("A master key has been set. Requests to Search Server won't be authorized unless you provide an authentication key.");
    //
    //         if master_key.len() < MASTER_KEY_MIN_SIZE {
    //             print_master_key_too_short_warning()
    //         }
    //     }
    //     ("development", None) => print_missing_master_key_warning(),
    //     // unreachable because Opt::try_build above would have failed already if any other value had been produced
    //     _ => unreachable!(),
    // }
}

const WARNING_BG_COLOR: Option<Color> = Some(Color::Ansi256(178));
const WARNING_FG_COLOR: Option<Color> = Some(Color::Ansi256(0));

fn print_master_key_too_short_warning() {
 //    let choice = if stderr().is_terminal() { ColorChoice::Auto } else { ColorChoice::Never };
 //    let mut stderr = StandardStream::stderr(choice);
 //    stderr
 //        .set_color(
 //            ColorSpec::new().set_bg(WARNING_BG_COLOR).set_fg(WARNING_FG_COLOR).set_bold(true),
 //        )
 //        .unwrap();
 //    writeln!(stderr, "\n").unwrap();
 //    writeln!(
 //        stderr,
 //        " Search Server started with a master key considered unsafe for use in a production environment.
 //
 // A master key of at least {MASTER_KEY_MIN_SIZE} bytes will be required when switching to a production environment."
 //    )
 //    .unwrap();
 //    stderr.reset().unwrap();
 //    writeln!(stderr).unwrap();
 //
 //    eprintln!("\n{}", generated_master_key_message());
 //    eprintln!(
 //        "\nRestart Search Server with the argument above to use this new and secure master key."
 //    )
}

fn print_missing_master_key_warning() {
//     let choice = if stderr().is_terminal() { ColorChoice::Auto } else { ColorChoice::Never };
//     let mut stderr = StandardStream::stderr(choice);
//     stderr
//         .set_color(
//             ColorSpec::new().set_bg(WARNING_BG_COLOR).set_fg(WARNING_FG_COLOR).set_bold(true),
//         )
//         .unwrap();
//     writeln!(stderr, "\n").unwrap();
//     writeln!(
//     stderr,
//     " No master key was found. The server will accept unidentified requests.
//
//  A master key of at least {MASTER_KEY_MIN_SIZE} bytes will be required when switching to a production environment."
// )
// .unwrap();
//     stderr.reset().unwrap();
//     writeln!(stderr).unwrap();
//
//     eprintln!("\n{}", generated_master_key_message());
//     eprintln!(
//         "\nRestart Search Server with the argument above to use this new and secure master key."
//     )
}

fn generated_master_key_message() -> String {
    format!(
        "We generated a new secure master key for you (you can safely use this token):

>> --master-key {} <<",
        generate_master_key()
    )
}
