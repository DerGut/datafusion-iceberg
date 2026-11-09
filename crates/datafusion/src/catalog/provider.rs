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

use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::error::Result;
use futures::future::try_join_all;
use iceberg::{Catalog, NamespaceIdent, SessionCatalog};

use super::adapter::SessionBindingCatalogAdapter;
use super::schema::IcebergSchemaProvider;
use crate::to_datafusion_error;

/// Provides an interface to manage and access schemas in an Iceberg [`SessionCatalog`].
///
/// Acts as a centralized catalog provider that aggregates
/// multiple [`SchemaProvider`], each associated with distinct namespaces.
#[derive(Debug)]
pub struct IcebergCatalogProvider {
    /// A `HashMap` where keys are namespace names
    /// and values are dynamic references to objects implementing the
    /// [`SchemaProvider`] trait.
    schemas: HashMap<String, Arc<dyn SchemaProvider>>,
}

impl IcebergCatalogProvider {
    /// Creates a provider backed by an Iceberg [`SessionCatalog`].
    ///
    /// Scans and inserts propagate the DataFusion session ID and configured
    /// [`crate::IcebergOptions`]. Without those options, they use the provider's
    /// shared anonymous fallback context.
    ///
    /// Provider initialization, metadata-table lookup, and SQL `CREATE TABLE`
    /// and `DROP TABLE` do not receive a DataFusion session and always use that
    /// fallback context. Namespace and table discovery happens once during
    /// construction and is shared by all sessions. The catalog must allow the
    /// fallback context to discover the intended namespaces, tables, and schemas;
    /// this provider does not implement per-session discovery or visibility.
    pub async fn try_new(catalog: Arc<dyn SessionCatalog>) -> Result<Self> {
        let client = Arc::new(SessionBindingCatalogAdapter::new(catalog));
        // TODO:
        // Schemas and providers should be cached and evicted based on time
        // As of right now; schemas might become stale.
        let schema_names: Vec<_> = client
            .list_namespaces(None)
            .await
            .map_err(to_datafusion_error)?
            .iter()
            .flat_map(|ns| ns.as_ref().clone())
            .collect();

        let providers = try_join_all(
            schema_names
                .iter()
                .map(|name| {
                    IcebergSchemaProvider::try_new(
                        client.clone(),
                        NamespaceIdent::new(name.clone()),
                    )
                })
                .collect::<Vec<_>>(),
        )
        .await?;

        let schemas: HashMap<String, Arc<dyn SchemaProvider>> = schema_names
            .into_iter()
            .zip(providers)
            .map(|(name, provider)| {
                let provider = Arc::new(provider) as Arc<dyn SchemaProvider>;
                (name, provider)
            })
            .collect();

        Ok(IcebergCatalogProvider { schemas })
    }
}

impl CatalogProvider for IcebergCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.get(name).cloned()
    }
}
