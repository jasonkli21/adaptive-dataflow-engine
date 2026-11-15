//! Feature-on downstream source compatibility smoke.

#[cfg(test)]
mod tests {
    #[test]
    fn ade_local_config_keeps_its_original_literal_shape() {
        let _config = ade_bench::ade_local::AdeLocalConfig {
            batch_size: 8192,
            execution: ade_exec::ExecutionConfig::default(),
        };
    }
}
