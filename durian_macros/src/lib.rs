use proc_macro::{self, TokenStream};
use proc_macro2::{Ident, Span};
use quote::quote;
use syn::{Data, DataStruct, Fields, parse_macro_input, DeriveInput};

#[proc_macro_attribute]
pub fn bincode_packet(_attr: TokenStream, tokens: TokenStream) -> TokenStream {
    let input = parse_macro_input!(tokens as DeriveInput);

    match &input.data {
        Data::Struct(data_struct) => {
            match &data_struct.fields {
                Fields::Named(fields) => {
                    if fields.named.is_empty() {
                        panic!("Struct annotated with #[bincode_packet] and has named fields cannot have empty number of fields")
                    }
                    // See https://github.com/serde-rs/serde/issues/1465 for why serde macro is needed
                    return quote! {
                        #[derive(durian::serde::Serialize, durian::serde::Deserialize, durian::BinPacket)]
                        #[serde(crate = "durian::serde")]
                        #input
                    }.into()
                },
                Fields::Unit => {
                    return quote! {
                        #[derive(durian::UnitPacket)]
                        #input
                    }.into()
                },
                _ => panic!("Only Structs with Named fields and Empty Unit structs can be annotated with #[bincode_packet]")
            }
        },
        _ => panic!("Only structs with named fields can be annotated with #[bincode_packet]"),
    };
}

#[proc_macro_derive(BinPacket)]
pub fn bin_packet(tokens: TokenStream) -> TokenStream {
    let input = parse_macro_input!(tokens as DeriveInput);
    let name = input.ident;
    let packet_builder_name = Ident::new((name.to_string() + "PacketBuilder").as_str(), Span::call_site());
    
    return quote! {
        impl durian::Packet for #name {
            fn as_bytes(&self) -> durian::bytes::Bytes {
                durian::bytes::Bytes::from(durian::bincode::serialize(self).unwrap())
            }
        }
        
        #[derive(Copy, Clone)]
        pub struct #packet_builder_name;
        impl durian::PacketBuilder<#name> for #packet_builder_name {
        
            fn read(&self, bytes: durian::bytes::Bytes) -> Result<#name, Box<dyn std::error::Error>> {
                Ok(durian::bincode::deserialize(bytes.as_ref()).unwrap())
            }
        }
    }.into();
}

#[proc_macro_derive(UnitPacket)]
pub fn unit_packet(tokens: TokenStream) -> TokenStream {
    let input = parse_macro_input!(tokens as DeriveInput);
    let name = input.ident;
    let name_str = format!("\"{}\"", name);
    let packet_builder_name = Ident::new((name.to_string() + "PacketBuilder").as_str(), Span::call_site());

    return quote! {
        impl durian::Packet for #name {
            fn as_bytes(&self) -> durian::bytes::Bytes {
                durian::bytes::Bytes::from(#name_str)
            }
        }
        
        #[derive(Copy, Clone)]
        pub struct #packet_builder_name;
        impl durian::PacketBuilder<#name> for #packet_builder_name {
        
            fn read(&self, bytes: durian::bytes::Bytes) -> Result<#name, Box<dyn std::error::Error>> {
                Ok(#name)
            }
        }
    }.into();
}

#[proc_macro_derive(ErrorOnlyMessage)]
pub fn error_only_message(tokens: TokenStream) -> TokenStream {
    let input = parse_macro_input!(tokens as DeriveInput);
    let name = input.ident;

    let fields_punct = match input.data {
        Data::Struct(DataStruct {
                         fields: Fields::Named(fields),
                         ..
                     }) => {
            fields.named
        },
        _ => panic!("Only structs with named fields can be annotated with ErrorMessageNew"),
    };

    let mut has_message = false;
    for field in fields_punct.iter() {
        if field.ident.as_ref().unwrap().to_string() == "message".to_string() {
            has_message = true;
            break;
        }
    }

    if !has_message {
        panic!("Only structs with a named field 'message: String' can be annotated with ErrorMessageNew")
    }

    let modified = quote! {
        impl #name {
            fn new<S: Into<String>>(message: S) -> Self {
                #name {
                    message: message.into()
                }
            }
        }
        
        impl std::error::Error for #name {}
    };
    modified.into()
}
