//! See the README for `wasm-bindgen-test` for a bit more info about what's
//! going on here.

extern crate proc_macro;

use proc_macro2::*;
use quote::quote;
use std::sync::atomic::*;

static CNT: AtomicUsize = AtomicUsize::new(0);

#[proc_macro_attribute]
pub fn wasm_bench(
    attr: proc_macro::TokenStream,
    body: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let mut attr = attr.into_iter();
    while let Some(_) = attr.next() {
        match &attr.next() {
            Some(proc_macro::TokenTree::Punct(op)) if op.as_char() == ',' => {}
            Some(_) => panic!("malformed `#[wasm_bindgen_test]` attribute"),
            None => break,
        }
    }

    let mut body = TokenStream::from(body).into_iter();

    // Skip over other attributes to `fn #ident ...`, and extract `#ident`
    let mut leading_tokens = Vec::new();
    while let Some(token) = body.next() {
        leading_tokens.push(token.clone());
        if let TokenTree::Ident(token) = token {
            if token == "fn" {
                break;
            }
        }
    }
    let ident = find_ident(&mut body).expect("expected a function name");

    // We generate a `#[no_mangle]` with a known prefix so the test harness can
    // later slurp up all of these functions and pass them as arguments to the
    // main test harness. This is the entry point for all tests.
    let name = format!("__wbgt_{}_{}", ident, CNT.fetch_add(1, Ordering::SeqCst));
    let name = Ident::new(&name, Span::call_site());
    let wrapper_name = Ident::new(&format!("{}_wrapper", ident), Span::call_site());
    let mut tokens = Vec::<TokenTree>::new();
    tokens.extend(
        (quote! {
            #[no_mangle]
            pub extern "C" fn #name(cx: &::wasm_bindgen_test::__rt::Context) {
                let test_name = concat!(module_path!(), "::", stringify!(#ident));
                crate::wasm::init_console_log();
                cx.execute_async(test_name, #wrapper_name);
            }

            async fn #wrapper_name() {
                let mut module_path = module_path!();
                let name: String = stringify!(#ident).into();
                let index = module_path.find("::").unwrap();
                module_path = &module_path[index + 2..];
                let test_name = match module_path.find("::") {
                    Some(index) => format!("{}::{}", &module_path[index + 2..], name),
                    None => name,
                };
                benchmark(&test_name, #ident).await;
            }
        })
        .into_iter(),
    );

    tokens.extend(leading_tokens);
    tokens.push(ident.into());
    tokens.extend(body);

    tokens.into_iter().collect::<TokenStream>().into()
}

fn find_ident(iter: &mut token_stream::IntoIter) -> Option<Ident> {
    match iter.next()? {
        TokenTree::Ident(i) => Some(i),
        TokenTree::Group(g) if g.delimiter() == Delimiter::None => {
            find_ident(&mut g.stream().into_iter())
        }
        _ => None,
    }
}
