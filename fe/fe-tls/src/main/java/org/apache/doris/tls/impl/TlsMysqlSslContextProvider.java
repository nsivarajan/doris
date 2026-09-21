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
import org.apache.doris.mysql.OssMysqlSslContextProvider;
import org.apache.doris.tls.server.TlsProtocolSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

/**
 * Provides SSL for the MySQL protocol port (9030).
 *
 * When enable_tls=true AND mysql not excluded AND tls_certificate_path is set:
 *   uses PEM certs for strict TLS (isClientUseSsl()=true on all connections).
 *
 * Otherwise: delegates to OssMysqlSslContextProvider (PKCS12 auto-generated certs).
 * This fallback is critical — without it, deployments that don't set enable_tls=true
 * would have this jar on the classpath (replacing OssMysqlSslContextProvider via SPI)
 * and then throw on blank cert paths, breaking MySQL startup and the ADD FOLLOWER pipeline.
 */
public class TlsMysqlSslContextProvider implements MysqlSslContextProvider {

    private static final Logger LOG = LogManager.getLogger(TlsMysqlSslContextProvider.class);

    private final MysqlSslContextProvider delegate;

    public TlsMysqlSslContextProvider() {
        boolean usePem = Config.enable_tls
                && TlsProtocolSet.isProtocolIncluded(TlsProtocolSet.Protocol.MYSQL)
                && Config.tls_certificate_path != null
                && !Config.tls_certificate_path.isBlank();
        if (usePem) {
            this.delegate = null;
            LOG.info("TLS: MySQL using PEM certs from tls_certificate_path={}",
                    Config.tls_certificate_path);
        } else {
            this.delegate = new OssMysqlSslContextProvider();
            LOG.info("TLS: MySQL using OSS PKCS12 certs (enable_tls={}, tls_certificate_path='{}')",
                    Config.enable_tls, Config.tls_certificate_path);
        }
    }

    @Override
    public SSLContext createSslContext(String protocol) throws Exception {
        if (delegate != null) {
            return delegate.createSslContext(protocol);
        }
        // Extra guard — should never reach here without a valid cert path,
        // but defend against Config not fully loaded at construction time.
        if (Config.tls_certificate_path == null || Config.tls_certificate_path.isBlank()) {
            LOG.warn("TLS: tls_certificate_path not set, falling back to OSS PKCS12 certs");
            return new OssMysqlSslContextProvider().createSslContext(protocol);
        }
        SSLContext ctx = TlsContextFactory.buildSslContext();
        LOG.info("TLS: MySQL SSLContext created from PEM, protocol={}", protocol);
        return ctx;
    }

    @Override
    public void configureEngine(SSLEngine engine) {
        if (delegate != null) {
            delegate.configureEngine(engine);
            return;
        }
        engine.setUseClientMode(false);
        String verifyMode = Config.tls_verify_mode;
        if ("verify_peer".equals(verifyMode)) {
            engine.setWantClientAuth(true);
        } else if ("verify_fail_if_no_peer_cert".equals(verifyMode)) {
            engine.setNeedClientAuth(true);
        } else {
            engine.setWantClientAuth(false);
        }
        engine.setEnabledProtocols(new String[]{"TLSv1.2", "TLSv1.3"});
    }
}
