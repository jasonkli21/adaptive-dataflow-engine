use std::net::Ipv4Addr;

pub(crate) fn public_endpoint(name: &str, address: Ipv4Addr) -> bool {
    let [a, b, c, _] = address.octets();
    !matches!(a, 0 | 10 | 127 | 224..=255)
        && !matches!(
            (a, b),
            (100, 64..=127) | (169, 254) | (172, 16..=31) | (192, 168) | (198, 18..=19)
        )
        && !matches!(
            (a, b, c),
            (192, 0, _) | (192, 88, 99) | (198, 51, 100) | (203, 0, 113)
        )
        && name.len() <= 253
        && name.contains('.')
        && name.parse::<Ipv4Addr>().is_err()
        && name.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label.as_bytes()[0].is_ascii_alphanumeric()
                && label.as_bytes()[label.len() - 1].is_ascii_alphanumeric()
                && label
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        })
}

#[cfg(any(feature = "flight", feature = "postgres"))]
pub(crate) async fn guarded<T>(
    context: &crate::Context,
    run: impl std::future::Future<Output = crate::Result<T>>,
) -> crate::Result<T> {
    tokio::pin!(run);
    loop {
        context.check()?;
        tokio::select! {
            result = &mut run => {
                context.check()?;
                return result;
            }
            () = tokio::time::sleep(std::time::Duration::from_millis(25)) => {}
        }
    }
}
