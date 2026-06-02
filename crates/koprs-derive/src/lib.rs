extern crate proc_macro;

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use syn::DeriveInput;

#[cfg(test)]
mod tests;

pub(crate) fn expand_ko_resource(_input: &DeriveInput) -> TokenStream2 {
    TokenStream2::new()
}

#[proc_macro_derive(KoResource)]
pub fn ko_resource_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse_macro_input!(input as DeriveInput);
    expand_ko_resource(&ast).into()
}
