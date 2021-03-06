/* tslint:disable */
/* eslint-disable */
/**
* @returns {Promise<any>}
*/
export function start(): Promise<any>;
/**
*/
export class LoaderHelper {
  free(): void;
/**
* @returns {string}
*/
  mainJS(): string;
}
/**
* A general-purpose thread pool for scheduling tasks that poll futures to
* completion.
*
* The thread pool multiplexes any number of tasks onto a fixed number of
* worker threads.
*
* This type is a clonable handle to the threadpool itself.
* Cloning it will only create a new reference, not a new threadpool.
*
* The API follows [`futures_executor::ThreadPool`].
*
* [`futures_executor::ThreadPool`]: https://docs.rs/futures-executor/0.3.16/futures_executor/struct.ThreadPool.html
*/
export class ThreadPool {
  free(): void;
}

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
  readonly start: () => number;
  readonly __wbg_threadpool_free: (a: number) => void;
  readonly loaderhelper_mainJS: (a: number) => number;
  readonly worker_entry_point: (a: number) => void;
  readonly __wbg_loaderhelper_free: (a: number) => void;
  readonly sqlite3_os_init: () => number;
  readonly wasm_vfs_currenttime: (a: number, b: number) => number;
  readonly wasm_vfs_sleep: (a: number, b: number) => number;
  readonly wasm_vfs_randomness: (a: number, b: number, c: number) => number;
  readonly wasm_vfs_dlclose: (a: number, b: number) => void;
  readonly wasm_vfs_dlsym: (a: number, b: number, c: number) => number;
  readonly wasm_vfs_dlerror: (a: number, b: number, c: number) => void;
  readonly wasm_vfs_dlopen: (a: number, b: number) => number;
  readonly wasm_vfs_access: (a: number, b: number, c: number, d: number) => number;
  readonly wasm_vfs_delete: (a: number, b: number, c: number) => number;
  readonly wasm_vfs_open: (a: number, b: number, c: number, d: number, e: number) => number;
  readonly malloc: (a: number) => number;
  readonly free: (a: number) => void;
  readonly realloc: (a: number, b: number) => number;
  readonly wasm_vfs_fullpathname: (a: number, b: number, c: number, d: number) => number;
  readonly memory: WebAssembly.Memory;
  readonly __wbindgen_malloc: (a: number) => number;
  readonly __wbindgen_realloc: (a: number, b: number, c: number) => number;
  readonly __wbindgen_export_3: WebAssembly.Table;
  readonly _dyn_core__ops__function__FnMut__A____Output___R_as_wasm_bindgen__closure__WasmClosure___describe__invoke__h068379d45c7b0649: (a: number, b: number, c: number) => void;
  readonly __wbindgen_free: (a: number, b: number) => void;
  readonly __wbindgen_exn_store: (a: number) => void;
  readonly wasm_bindgen__convert__closures__invoke2_mut__h94294d14e821a05e: (a: number, b: number, c: number, d: number) => void;
  readonly __wbindgen_thread_destroy: () => void;
  readonly __wbindgen_start: () => void;
}

/**
* Synchronously compiles the given `bytes` and instantiates the WebAssembly module.
*
* @param {BufferSource} bytes
* @param {WebAssembly.Memory} maybe_memory
*
* @returns {InitOutput}
*/
export function initSync(bytes: BufferSource, maybe_memory?: WebAssembly.Memory): InitOutput;

/**
* If `module_or_path` is {RequestInfo} or {URL}, makes a request and
* for everything else, calls `WebAssembly.instantiate` directly.
*
* @param {InitInput | Promise<InitInput>} module_or_path
* @param {WebAssembly.Memory} maybe_memory
*
* @returns {Promise<InitOutput>}
*/
export default function init (module_or_path?: InitInput | Promise<InitInput>, maybe_memory?: WebAssembly.Memory): Promise<InitOutput>;
