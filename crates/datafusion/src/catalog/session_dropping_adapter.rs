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

use async_trait::async_trait;
use iceberg::table::Table;
use iceberg::{
    Catalog, Namespace, NamespaceIdent, Result, SessionCatalog, SessionContext,
    TableCommit, TableCreation, TableIdent,
};

/// A wrapper around a [`Catalog`] to provide a [`SessionCatalog`] API by
/// ignoring any passed [`SessionContext`].
///
/// Use this explicitly for catalogs that do not need session-specific behavior,
/// such as an in-memory catalog. Identity, credentials, and properties supplied
/// by DataFusion are discarded; this adapter provides no tenant isolation.
#[derive(Debug)]
pub struct SessionDroppingCatalogAdapter {
    inner: Arc<dyn Catalog>,
}

impl SessionDroppingCatalogAdapter {
    /// Wraps a catalog whose operations are independent of the query session.
    pub fn new(inner: Arc<dyn Catalog>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl SessionCatalog for SessionDroppingCatalogAdapter {
    async fn list_namespaces(
        &self,
        _: &SessionContext,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        _: &SessionContext,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }

    async fn get_namespace(
        &self,
        _: &SessionContext,
        namespace: &NamespaceIdent,
    ) -> Result<Namespace> {
        self.inner.get_namespace(namespace).await
    }

    async fn namespace_exists(
        &self,
        _: &SessionContext,
        ns: &NamespaceIdent,
    ) -> Result<bool> {
        self.inner.namespace_exists(ns).await
    }

    async fn update_namespace(
        &self,
        _: &SessionContext,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        self.inner.update_namespace(namespace, properties).await
    }

    async fn drop_namespace(
        &self,
        _: &SessionContext,
        namespace: &NamespaceIdent,
    ) -> Result<()> {
        self.inner.drop_namespace(namespace).await
    }

    async fn list_tables(
        &self,
        _: &SessionContext,
        namespace: &NamespaceIdent,
    ) -> Result<Vec<TableIdent>> {
        self.inner.list_tables(namespace).await
    }

    async fn create_table(
        &self,
        _: &SessionContext,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        self.inner.create_table(namespace, creation).await
    }

    async fn load_table(
        &self,
        _: &SessionContext,
        table_ident: &TableIdent,
    ) -> Result<Table> {
        self.inner.load_table(table_ident).await
    }

    async fn drop_table(&self, _: &SessionContext, table: &TableIdent) -> Result<()> {
        self.inner.drop_table(table).await
    }

    async fn purge_table(&self, _: &SessionContext, table: &TableIdent) -> Result<()> {
        self.inner.purge_table(table).await
    }

    async fn table_exists(&self, _: &SessionContext, table: &TableIdent) -> Result<bool> {
        self.inner.table_exists(table).await
    }

    async fn rename_table(
        &self,
        _: &SessionContext,
        src: &TableIdent,
        dest: &TableIdent,
    ) -> Result<()> {
        self.inner.rename_table(src, dest).await
    }

    async fn register_table(
        &self,
        _: &SessionContext,
        table_ident: &TableIdent,
        metadata_location: String,
    ) -> Result<Table> {
        self.inner
            .register_table(table_ident, metadata_location)
            .await
    }

    async fn update_table(
        &self,
        _: &SessionContext,
        commit: TableCommit,
    ) -> Result<Table> {
        self.inner.update_table(commit).await
    }
}
