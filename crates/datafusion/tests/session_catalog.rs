// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::CatalogProvider;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::execution::config::SessionConfig;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext as DFSessionContext;
use iceberg::sensitive::SensitiveString;
use iceberg::{ErrorKind, SessionCatalog};

use iceberg_datafusion::{
    IcebergCatalogProvider, IcebergCatalogTableProvider, IcebergOptions,
    from_datafusion_error,
};

#[path = "session_catalog/recording.rs"]
mod test_utils;

#[tokio::test]
async fn test_rebuilt_provider_preserves_session_catalog() {
    let (catalog, namespace, table_name, _temp_dir) =
        test_utils::create_recording_catalog().await;
    let session_catalog: Arc<dyn SessionCatalog> = catalog.clone();
    let provider = IcebergCatalogTableProvider::try_new(
        session_catalog.clone(),
        namespace,
        table_name,
    )
    .await
    .unwrap();
    assert!(Arc::ptr_eq(provider.catalog(), &session_catalog));
    let rebuilt = IcebergCatalogTableProvider::try_new(
        provider.catalog().clone(),
        provider.table_ident().namespace().clone(),
        provider.table_ident().name(),
    )
    .await
    .unwrap();
    catalog.clear_calls();

    let options = test_utils::iceberg_options();
    let context = DFSessionContext::new_with_config(
        SessionConfig::new().with_option_extension(options.clone()),
    );
    rebuilt
        .scan(&context.state(), None, &[], None)
        .await
        .unwrap();
    let calls = catalog.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].session_id, context.session_id());
    assert_eq!(calls[0].identity, options.identity);
    assert_eq!(calls[0].properties, options.properties);
    assert_eq!(calls[0].credentials, options.credentials);
}

#[tokio::test]
async fn test_standalone_provider_default_options_use_query_session() {
    let (catalog, namespace, table_name, _temp_dir) =
        test_utils::create_recording_catalog().await;
    let provider =
        IcebergCatalogTableProvider::try_new(catalog.clone(), namespace, table_name)
            .await
            .unwrap();
    let fallback_id = catalog.calls()[0].session_id.clone();
    catalog.clear_calls();

    let context = DFSessionContext::new_with_config(
        SessionConfig::new().with_option_extension(IcebergOptions::default()),
    );
    provider
        .scan(&context.state(), None, &[], None)
        .await
        .unwrap();
    // A query without options continues to use the shared fallback.
    provider
        .scan(&DFSessionContext::new().state(), None, &[], None)
        .await
        .unwrap();

    let calls = catalog.calls();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].session_id, context.session_id());
    assert_ne!(calls[0].session_id, fallback_id);
    assert_eq!(calls[1].session_id, fallback_id);
    assert!(calls.iter().all(|call| call.identity.is_none()
        && call.properties.is_empty()
        && call.credentials.is_empty()));
}

#[tokio::test]
async fn test_catalog_errors_are_propagated_without_fallback() {
    use datafusion::logical_expr::dml::InsertOp;
    use datafusion::physical_plan::empty::EmptyExec;

    let (catalog, namespace, table_name, _temp_dir) =
        test_utils::create_recording_catalog().await;
    let provider =
        IcebergCatalogTableProvider::try_new(catalog.clone(), namespace, table_name)
            .await
            .unwrap();
    catalog.clear_calls();
    catalog.reject_loads();
    let context = DFSessionContext::new_with_config(
        SessionConfig::new().with_option_extension(test_utils::iceberg_options()),
    );
    let state = context.state();
    let errors = [
        provider.scan(&state, None, &[], None).await.unwrap_err(),
        provider
            .insert_into(
                &state,
                Arc::new(EmptyExec::new(provider.schema())),
                InsertOp::Append,
            )
            .await
            .unwrap_err(),
    ];
    for error in errors {
        let error = from_datafusion_error(error);
        assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(error.message(), "load denied");
    }
    let calls = catalog.calls();
    assert_eq!(calls.len(), 2);
    assert!(calls.iter().all(|call| call.operation == "load_table"
        && call.session_id == context.session_id()
        && call.identity == test_utils::iceberg_options().identity));
}

#[tokio::test]
async fn test_sql_configuration_redacts_credentials_in_settings() {
    let context = DFSessionContext::new_with_config(
        SessionConfig::new()
            .with_information_schema(true)
            .with_option_extension(IcebergOptions::default()),
    );
    for sql in [
        "SET iceberg.identity = 'sql-user'",
        "SET iceberg.properties.s3.region = 'eu-west-1'",
        "SET iceberg.credentials.token = 'secret-not-for-settings'",
    ] {
        context.sql(sql).await.unwrap().collect().await.unwrap();
    }
    let batches = context
        .sql(
            "SELECT name, value FROM information_schema.df_settings \
             WHERE name LIKE 'iceberg.%' ORDER BY name",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    datafusion::assert_batches_eq!(
        [
            "+------------------------------+------------+",
            "| name                         | value      |",
            "+------------------------------+------------+",
            "| iceberg.credentials.token    | [REDACTED] |",
            "| iceberg.identity             | sql-user   |",
            "| iceberg.properties.s3.region | eu-west-1  |",
            "+------------------------------+------------+",
        ],
        &batches
    );

    let (catalog, namespace, table_name, _temp_dir) =
        test_utils::create_recording_catalog().await;
    let provider =
        IcebergCatalogTableProvider::try_new(catalog.clone(), namespace, table_name)
            .await
            .unwrap();
    catalog.clear_calls();
    provider
        .scan(&context.state(), None, &[], None)
        .await
        .unwrap();
    let calls = catalog.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].identity.as_deref(), Some("sql-user"));
    assert_eq!(
        calls[0].credentials["token"].expose(),
        "secret-not-for-settings"
    );
    assert_eq!(calls[0].properties["s3.region"], "eu-west-1");
}

#[tokio::test]
async fn test_session_aware_scan_uses_resolved_context() {
    let (session_catalog, namespace, table_name, _temp_dir) =
        test_utils::create_recording_catalog().await;
    let provider = IcebergCatalogProvider::try_new(session_catalog.clone())
        .await
        .unwrap();

    let bootstrap_calls = session_catalog.calls();
    assert_eq!(
        bootstrap_calls
            .iter()
            .map(|call| call.operation)
            .collect::<Vec<_>>(),
        vec!["list_namespaces", "list_tables", "load_table"]
    );
    session_catalog.clear_calls();

    let fallback_session_id = bootstrap_calls[0].session_id.as_str();
    assert!(bootstrap_calls.iter().all(|call| {
        call.session_id == fallback_session_id
            && call.identity.is_none()
            && call.properties.is_empty()
            && call.credentials.is_empty()
    }));

    let schema = provider.schema(namespace[0].as_str()).unwrap();
    let table = schema.table(&table_name).await.unwrap().unwrap();

    let first_options = test_utils::iceberg_options();
    let config = SessionConfig::new().with_option_extension(first_options.clone());
    let first_df_context = DFSessionContext::new_with_config(config);

    table
        .scan(&first_df_context.state(), None, &[], None)
        .await
        .unwrap();

    let second_options = IcebergOptions {
        identity: Some("another-user".to_string()),
        properties: HashMap::from([(
            "another-property".to_string(),
            "another-value".to_string(),
        )]),
        credentials: HashMap::from([(
            "another-token".to_string(),
            SensitiveString::from("another-secret".to_string()),
        )]),
    };
    let config = SessionConfig::new().with_option_extension(second_options.clone());
    let second_df_context = DFSessionContext::new_with_config(config);

    table
        .scan(&second_df_context.state(), None, &[], None)
        .await
        .unwrap();

    let calls = session_catalog.calls();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].operation, "load_table");
    assert_eq!(calls[0].session_id, first_df_context.session_id());
    assert_eq!(calls[0].identity, first_options.identity);
    assert_eq!(calls[0].properties, first_options.properties);
    assert_eq!(calls[0].credentials, first_options.credentials);

    assert_eq!(calls[1].operation, "load_table");
    assert_eq!(calls[1].session_id, second_df_context.session_id());
    assert_eq!(calls[1].identity, second_options.identity);
    assert_eq!(calls[1].properties, second_options.properties);
    assert_eq!(calls[1].credentials, second_options.credentials);
}

#[tokio::test]
async fn test_session_aware_scan_without_options_uses_fallback_context() {
    let (session_catalog, namespace, table_name, _temp_dir) =
        test_utils::create_recording_catalog().await;
    let provider = IcebergCatalogProvider::try_new(session_catalog.clone())
        .await
        .unwrap();
    let fallback_session_id = session_catalog.calls()[0].session_id.clone();
    let schema = provider.schema(namespace[0].as_str()).unwrap();
    let table = schema.table(&table_name).await.unwrap().unwrap();
    session_catalog.clear_calls();

    let df_context = DFSessionContext::new();
    table
        .scan(&df_context.state(), None, &[], None)
        .await
        .unwrap();

    let calls = session_catalog.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].operation, "load_table");
    assert_eq!(calls[0].session_id, fallback_session_id);
    assert_ne!(calls[0].session_id, df_context.session_id());
    assert!(calls[0].identity.is_none());
    assert!(calls[0].properties.is_empty());
    assert!(calls[0].credentials.is_empty());
}

#[tokio::test]
async fn test_session_aware_insert_reuses_resolved_context_for_commit() {
    let (session_catalog, namespace, table_name, _temp_dir) =
        test_utils::create_recording_catalog().await;
    let provider = IcebergCatalogProvider::try_new(session_catalog.clone())
        .await
        .unwrap();
    let schema = provider.schema(namespace[0].as_str()).unwrap();
    let table = schema.table(&table_name).await.unwrap().unwrap();
    session_catalog.clear_calls();

    let config =
        SessionConfig::new().with_option_extension(test_utils::iceberg_options());
    let df_context = DFSessionContext::new_with_config(config);
    df_context
        .register_table("test_table", table.clone())
        .unwrap();
    let plan = df_context
        .sql("INSERT INTO test_table VALUES (1, 'test')")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();

    // Reusing the provider for another session must not rebind the planned insert.
    let other_context = DFSessionContext::new_with_config(
        SessionConfig::new().with_option_extension(IcebergOptions {
            identity: Some("other-user".to_string()),
            ..Default::default()
        }),
    );
    table
        .scan(&other_context.state(), None, &[], None)
        .await
        .unwrap();
    assert_eq!(
        session_catalog.calls().last().unwrap().identity.as_deref(),
        Some("other-user")
    );
    session_catalog.clear_calls();
    session_catalog.fail_next_commit();

    // Even execution with another task context and a commit retry must use the
    // original planning context for every catalog request.
    collect(plan, other_context.task_ctx()).await.unwrap();

    let calls = session_catalog.calls();
    assert_eq!(
        calls
            .iter()
            .filter(|call| call.operation == "update_table")
            .count(),
        2
    );
    let load = calls
        .iter()
        .find(|call| call.operation == "load_table")
        .unwrap();
    let update = calls
        .iter()
        .find(|call| call.operation == "update_table")
        .unwrap();
    assert_eq!(update.session_id, load.session_id);
    assert_eq!(update.identity, load.identity);
    assert_eq!(update.properties, load.properties);
    assert_eq!(update.credentials, load.credentials);
    assert_eq!(load.session_id, df_context.session_id());
    assert_eq!(load.identity, test_utils::iceberg_options().identity);
    assert_eq!(load.properties, test_utils::iceberg_options().properties);
    assert_eq!(load.credentials, test_utils::iceberg_options().credentials);
    assert!(calls.iter().all(|call| {
        call.session_id == load.session_id
            && call.identity == load.identity
            && call.properties == load.properties
            && call.credentials == load.credentials
    }));

    let batches = df_context
        .sql("SELECT * FROM test_table")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    datafusion::assert_batches_eq!(
        [
            "+----+------+",
            "| id | name |",
            "+----+------+",
            "| 1  | test |",
            "+----+------+"
        ],
        &batches
    );
}

#[tokio::test]
async fn test_sessionless_schema_operations_share_fallback_context() {
    let (session_catalog, namespace, table_name, _temp_dir) =
        test_utils::create_recording_catalog().await;
    let provider = IcebergCatalogProvider::try_new(session_catalog.clone())
        .await
        .unwrap();
    let schema = provider.schema(namespace[0].as_str()).unwrap();
    session_catalog.clear_calls();

    schema
        .table(&format!("{table_name}$snapshots"))
        .await
        .unwrap()
        .unwrap();

    let metadata_calls = session_catalog.calls();
    assert_eq!(metadata_calls.len(), 1);
    assert_eq!(metadata_calls[0].operation, "load_table");
    let fallback_session_id = metadata_calls[0].session_id.clone();
    assert!(metadata_calls.iter().all(|call| {
        call.session_id == fallback_session_id
            && call.identity.is_none()
            && call.properties.is_empty()
            && call.credentials.is_empty()
    }));
    session_catalog.clear_calls();

    let arrow_schema = schema.table(&table_name).await.unwrap().unwrap().schema();
    let empty_batch = RecordBatch::new_empty(arrow_schema.clone());
    let empty_table = MemTable::try_new(arrow_schema, vec![vec![empty_batch]]).unwrap();
    schema
        .register_table("registered_table".to_string(), Arc::new(empty_table))
        .unwrap();
    let mut calls = session_catalog.calls();
    session_catalog.clear_calls();

    // Tables created through the fallback must still bind later query sessions.
    let context = DFSessionContext::new_with_config(
        SessionConfig::new().with_option_extension(test_utils::iceberg_options()),
    );
    schema
        .table("registered_table")
        .await
        .unwrap()
        .unwrap()
        .scan(&context.state(), None, &[], None)
        .await
        .unwrap();
    let scan_calls = session_catalog.calls();
    assert_eq!(scan_calls.len(), 1);
    assert_eq!(scan_calls[0].session_id, context.session_id());
    assert_eq!(
        scan_calls[0].identity,
        test_utils::iceberg_options().identity
    );
    session_catalog.clear_calls();
    schema.deregister_table("registered_table").unwrap();

    calls.extend(session_catalog.calls());
    assert!(calls.iter().any(|call| call.operation == "create_table"));
    assert!(calls.iter().any(|call| call.operation == "drop_table"));
    assert!(calls.iter().all(|call| {
        call.session_id == fallback_session_id
            && call.identity.is_none()
            && call.properties.is_empty()
            && call.credentials.is_empty()
    }));
}
