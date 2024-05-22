use std::ffi::OsStr;
use std::fmt::{Display, Formatter, Write};
use std::process::Stdio;
use actix_web::{ HttpResponse, web};
use actix_web::http::header::ContentType;
use deserr::actix_web::{AwebJson, AwebQueryParameter};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use shared_types::deserr::{DeserrJsonError, DeserrQueryParamError};
use shared_types::error::{Code, ResponseError};


pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::post().to(render_graph)));
}

#[derive(Debug, deserr::Deserr, Default)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
pub enum Layout{
    #[default]
    ELK,
    Dagre
}


#[derive(Debug, deserr::Deserr)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
pub struct GraphData {
    graph: String,
    #[serde(default)]
    #[deserr(default, error = DeserrJsonError)]
    layout: Option<Layout>
}

pub async fn render_graph(
    params: AwebJson<GraphData, DeserrJsonError>,
) -> Result<HttpResponse, ResponseError> {
    let GraphData{ graph, layout } = params.into_inner();
    let layout = layout.unwrap_or(Layout::ELK).to_string();

    let args = [
        OsStr::new("--layout"),
        layout.as_ref(),
        OsStr::new("-"),
    ];

    let mut child = Command::new("d2")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .args(args)
        .spawn()?;

    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(graph.as_bytes())
        .await?;

    let output = child
        .wait_with_output()
        .await?;

    if output.status.success() {
        let diagram = String::from_utf8_lossy(&output.stdout).to_string();

        return Ok(HttpResponse::Ok()
            .content_type(ContentType(mime::IMAGE_SVG))
            .body(diagram))

    } else {
        let src =
            format!("\n{}", String::from_utf8_lossy(&output.stderr)).replace('\n', "\n  ");

        let msg = format!(
            "Failed to compile D2 diagram: {src}");

        return Err(ResponseError::from_msg(msg, Code::FailedToCompileD2))
    }


}


impl Display for Layout{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Layout::ELK => { f.write_str("elk") }
            Layout::Dagre => { f.write_str("dagre") }
        }
    }
}