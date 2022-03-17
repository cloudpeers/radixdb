use std::time::Duration;

use futures::{channel::mpsc, executor::block_on};
use futures::StreamExt;
use log::*;
use wasm_bindgen::{prelude::*, JsCast};
use wasm_futures_executor::ThreadPool;
use web_sys::DedicatedWorkerGlobalScope;

#[wasm_bindgen(start)]
pub fn main() {
    let _ = console_log::init_with_level(log::Level::Debug);
    ::console_error_panic_hook::set_once();
}

fn worker_name() -> String {
    js_sys::global()
        .unchecked_into::<DedicatedWorkerGlobalScope>()
        .name()
        .as_str()
        .to_string()
}

fn err_to_jsvalue(value: impl std::error::Error) -> JsValue {
    value.to_string().into()
}

async fn w2() -> String {
    worker_name()
}

#[wasm_bindgen]
pub async fn start() -> Result<JsValue, JsValue> {
    let pool = ThreadPool::new(2).await?;
    let pool2 = pool.clone();
    let res = pool.spawn(async move {    
        let fut = pool2.spawn(w2());
        let name = block_on(fut).unwrap();
        format!("{} {}", worker_name(), name)
    }).await.map_err(err_to_jsvalue)?;    
    Ok(res.into())
}
