// src/tests/ko_resource.rs
#[cfg(test)]
mod ko_resource_tests {
    use proc_macro2::TokenStream;
    use quote::quote;

    use crate::expand_ko_resource;

    fn parse(ts: TokenStream) -> syn::DeriveInput {
        syn::parse2(ts).expect("failed to parse test input")
    }

    #[test]
    fn expand_produces_no_tokens_for_stub() {
        let input = parse(quote! {
            struct MyResource {
                metadata: String,
            }
        });
        let output = expand_ko_resource(&input);
        assert!(output.is_empty());
    }

    #[test]
    fn expand_accepts_struct_with_named_fields() {
        let input = parse(quote! {
            struct Foo {
                a: u32,
                b: String,
            }
        });
        let output = expand_ko_resource(&input);
        assert!(output.is_empty());
    }

    #[test]
    fn expand_accepts_unit_struct() {
        let input = parse(quote! { struct Bar; });
        let output = expand_ko_resource(&input);
        assert!(output.is_empty());
    }
}
