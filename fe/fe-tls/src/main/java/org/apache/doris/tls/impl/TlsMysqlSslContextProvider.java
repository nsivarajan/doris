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

package org.apache.doris.tls.impl;

import org.apache.doris.common.Config;
import org.apache.doris.mysql.MysqlSslContextProvider;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

/**
 * Provides SSL for the MySQL protocol port (9030) when enable_tls=true and mysql
 * is not in tls_excluded_protocols.
 *
 * Uses PEM certs from tls_certificate_path / tls_private_key_path / tls_ca_certificate_path.
 * This replaces OssMysqlSslContextProvider which uses the older PKCS12 auto-generated certs.
 *
 * With this in place, ALL MySQL connections use TLS — clients do not need
 * --ssl-mode=REQUIRED because the server always negotiates SSL. This means
 * isClientUseSsl()=true on every connection, so OIDC tokens are accepted
 * without any explicit SSL flag on the client side.
 */
public class TlsMysqlSslContextProvider implements MysqlSslContextProvider {

    private static final Logger LOG = LogManager.getLogger(TlsMysqlSslContextProvider.class);

    @Override
    public SSLContext createSslContext(String protocol) throws Exception {
        SSLContext ctx = TlsContextFactory.buildSslContext();
        LOG.info("TLS: MySQL SSLContext created, protocol={}", protocol);
        return ctx;
    }

    @Override
    public void configureEngine(SSLEngine engine) {
        engine.setUseClientMode(false);

        String verifyMode = Config.tls_verify_mode;
        if ("verify_peer".equals(verifyMode)) {
            // Request client cert but don't require it — MySQL clients without certs still connect
            engine.setWantClientAuth(true);
        } else if ("verify_fail_if_no_peer_cert".equals(verifyMode)) {
            // Strict mTLS — client cert is mandatory
            engine.setNeedClientAuth(true);
        } else {
            // verify_none — encrypt only, no client cert
            engine.setWantClientAuth(false);
        }

        engine.setEnabledProtocols(new String[]{"TLSv1.2", "TLSv1.3"});
    }
}
