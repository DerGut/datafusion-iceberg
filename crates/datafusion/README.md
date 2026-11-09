<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Apache Iceberg DataFusion Integration

This crate contains the integration of Apache DataFusion and Apache Iceberg.

## Session-aware catalogs

Use `IcebergCatalogProvider::try_new` to connect an Iceberg
`SessionCatalog`, such as `iceberg_catalog_rest::RestSessionCatalog`. For an
individual table, use `IcebergCatalogTableProvider::try_new`.
Both constructors require `Arc<dyn SessionCatalog>`. The table provider's
`catalog()` accessor returns the original session catalog, so rebuilding a
provider preserves session propagation.

To use a catalog that does not support sessions, explicitly wrap it in
`SessionDroppingCatalogAdapter` before passing it to a provider. This adapter
ignores all query context and is appropriate only when catalog operations do not
need tenant-specific behavior:

```rust
use std::sync::Arc;
use iceberg::{Catalog, SessionCatalog};
use iceberg_datafusion::SessionDroppingCatalogAdapter;

fn adapt(catalog: Arc<dyn Catalog>) -> Arc<dyn SessionCatalog> {
    Arc::new(SessionDroppingCatalogAdapter::new(catalog))
}
```

Prefer a native session catalog, such as `RestSessionCatalog`, when the catalog
uses identity or credentials. The commit operator still accepts a plain
`Catalog`; insert planning binds the query context before passing it there.

Register `IcebergOptions` with DataFusion's `SessionConfig::with_option_extension`:

```rust
use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg_datafusion::IcebergOptions;

let config = SessionConfig::new().with_option_extension(IcebergOptions {
    identity: Some("alice".to_string()),
    ..Default::default()
});
let context = SessionContext::new_with_config(config);
```

Scans and inserts pass the DataFusion session ID, identity, properties, and
credentials to the session catalog. An insert keeps the context resolved during
planning through execution and commit retries. Use separate DataFusion sessions
for distinct identities, since catalogs may cache authentication by session ID.

Catalog construction discovers namespaces, tables, and schemas once using a
shared anonymous fallback context. Metadata-table lookup and SQL `CREATE TABLE`
and `DROP TABLE` also use this fallback because their DataFusion provider APIs
do not receive a session. The discovery set is shared by all sessions: this
provider does not provide per-session catalog visibility. The catalog must allow
the fallback context to perform these operations. Scans and inserts without an
`IcebergOptions` extension also use the fallback; registering default options
instead supplies the DataFusion session ID with no identity or credentials.

String configuration and SQL `SET` support `iceberg.identity`,
`iceberg.properties.<key>`, and `iceberg.credentials.<key>`. Applications that
accept untrusted SQL must control which settings users can change. Credentials
are redacted in debug output and configuration listings, including
`information_schema.df_settings`. Configuration listings cannot round-trip
credentials; applications that serialize them must explicitly read the typed
`IcebergOptions::credentials` map.

The self-contained [session catalog example](examples/session_catalog.rs) uses
an in-memory catalog to show context propagation without an external service:

```shell
cargo run -p iceberg-datafusion --example session_catalog
```
