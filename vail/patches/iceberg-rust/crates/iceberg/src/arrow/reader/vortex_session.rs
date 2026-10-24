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

use std::sync::Mutex;

use vortex::VortexSessionDefault;
use vortex::session::VortexSession;

use crate::{Error, ErrorKind, Result};

/// Bounded preparation shared by readers using the default Vortex registry.
///
/// The session is initialized lazily in the runtime polling a file read. Only
/// one runtime/session pair is retained; switching runtimes replaces the cache
/// entry, while active readers keep their own cloned sessions. Mutable file,
/// scan, expression-execution and metrics state remains owned by each reader.
///
/// This cache only constructs the pinned default configuration. Its Tokio
/// RuntimeSession uses Vortex's static CurrentTokioRuntime dispatcher, which
/// resolves Tokio's current handle for each operation. The ID is a conservative
/// cache-selection boundary, not proof that a runtime is still alive; even a
/// reused ID cannot resurrect a captured runtime here. Configured sessions must
/// not be inserted into this cache.
#[derive(Debug, Default)]
pub struct VortexSessionCache(Mutex<Option<(tokio::runtime::Id, VortexSession)>>);

impl VortexSessionCache {
    pub(super) fn current(&self) -> Result<VortexSession> {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            // Preserve the existing default-session behavior for executors
            // without a current Tokio runtime; never cache an unbound session.
            return Ok(VortexSession::default());
        };
        let runtime = handle.id();
        let mut cached = self
            .0
            .lock()
            .map_err(|_| Error::new(ErrorKind::Unexpected, "Vortex session cache was poisoned"))?;
        if let Some((id, session)) = cached.as_ref() {
            if *id == runtime {
                return Ok(session.clone());
            }
        }
        let session = VortexSession::default();
        *cached = Some((runtime, session.clone()));
        Ok(session)
    }
}
