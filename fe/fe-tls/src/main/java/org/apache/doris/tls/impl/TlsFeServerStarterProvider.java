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
import org.apache.doris.common.ThriftServer;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mysql.MysqlServer;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectScheduler;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.service.FrontendServiceImpl;
import org.apache.doris.service.arrowflight.DorisFlightSqlService;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.tls.server.FeServerStarterProvider;
import org.apache.doris.tls.server.ServerStarter;
import org.apache.doris.tls.server.TlsProtocolSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TProcessor;

import java.lang.reflect.Proxy;
import javax.net.ssl.SSLContext;

/**
 * TLS-capable implementations of MySQL, Thrift, and Arrow Flight server starters.
 *
 * Selected by ServiceLoader when this jar is on the classpath, replacing
 * OssFeServerStarterProvider. Each starter respects tls_excluded_protocols —
 * if a protocol is excluded, it falls back to the plain (non-TLS) behaviour.
 */
public class TlsFeServerStarterProvider implements FeServerStarterProvider {

    private static final Logger LOG = LogManager.getLogger(TlsFeServerStarterProvider.class);

    @Override
    public ServerStarter createThriftServerStarter(int port) {
        return new TlsThriftServerStarter(port);
    }

    @Override
    public ServerStarter createMysqlServerStarter(int port, ConnectScheduler scheduler) {
        return new TlsMysqlServerStarter(port, scheduler);
    }

    @Override
    public ServerStarter createFlightServerStarter(int port) {
        return new TlsFlightServerStarter(port);
    }

    // ── MySQL ─────────────────────────────────────────────────────────────────

    private static final class TlsMysqlServerStarter implements ServerStarter {
        private final MysqlServer mysqlServer;
        private final int port;

        private TlsMysqlServerStarter(int port, ConnectScheduler scheduler) {
            this.port = port;
            this.mysqlServer = new MysqlServer(port, scheduler);
        }

        @Override
        public void start() {
            if (Config.enable_tls
                    && !TlsProtocolSet.isProtocolIncluded(TlsProtocolSet.Protocol.MYSQL)) {
                LOG.info("TLS: MySQL excluded from tls_excluded_protocols — starting plain");
            } else {
                LOG.info("TLS: Starting MySQL server with TLS on port {}", port);
            }
            if (!mysqlServer.start()) {
                throw new IllegalStateException("MySQL server failed to start");
            }
        }

        @Override
        public void stop() {
            mysqlServer.stop();
        }
    }

    // ── Thrift ────────────────────────────────────────────────────────────────

    private static final class TlsThriftServerStarter implements ServerStarter {
        private final int port;
        private ThriftServer server;

        private TlsThriftServerStarter(int port) {
            this.port = port;
        }

        @Override
        public void start() throws Exception {
            if (!Config.enable_tls
                    || !TlsProtocolSet.isProtocolIncluded(TlsProtocolSet.Protocol.THRIFT)) {
                LOG.info("TLS: Thrift excluded or enable_tls=false — starting plain on port {}", port);
                startPlain();
                return;
            }

            LOG.info("TLS: Starting Thrift server with TLS on port {}", port);
            // Thrift TLS via TSSLTransportParameters requires cert/key in specific formats.
            // We wrap the ThriftServer startup with SSLContext injection.
            // For now falls back to plain — full Thrift TLS needs TSSLTransportFactory integration.
            // TODO: integrate TSSLTransportFactory with TlsContextFactory when required.
            LOG.warn("TLS: Thrift TLS not yet fully implemented — starting plain. "
                    + "Add 'thrift' to tls_excluded_protocols to suppress this warning.");
            startPlain();
        }

        private void startPlain() throws Exception {
            FrontendServiceImpl service = new FrontendServiceImpl(ExecuteEnv.getInstance());
            Logger feServiceLogger = LogManager.getLogger(FrontendServiceImpl.class);
            FrontendService.Iface instance = (FrontendService.Iface) Proxy.newProxyInstance(
                    FrontendServiceImpl.class.getClassLoader(),
                    FrontendServiceImpl.class.getInterfaces(),
                    (proxy, method, args) -> {
                        long begin = System.currentTimeMillis();
                        String name = method.getName();
                        if (MetricRepo.isInit) {
                            MetricRepo.THRIFT_COUNTER_RPC_ALL.getOrAdd(name).increase(1L);
                        }
                        feServiceLogger.debug("receive request for {}", name);
                        try {
                            return method.invoke(service, args);
                        } finally {
                            ConnectContext.remove();
                            feServiceLogger.debug("finish process request for {}", name);
                            if (MetricRepo.isInit) {
                                MetricRepo.THRIFT_COUNTER_RPC_LATENCY.getOrAdd(name)
                                        .increase(System.currentTimeMillis() - begin);
                            }
                        }
                    });
            TProcessor processor = new FrontendService.Processor<>(instance);
            server = new ThriftServer(port, processor);
            server.start();
        }

        @Override
        public void stop() throws Exception {
            if (server != null) {
                server.stop();
                server.join();
            }
        }
    }

    // ── Arrow Flight ──────────────────────────────────────────────────────────

    private static final class TlsFlightServerStarter implements ServerStarter {
        private final int port;
        private DorisFlightSqlService flightSqlService;

        private TlsFlightServerStarter(int port) {
            this.port = port;
        }

        @Override
        public void start() {
            if (port == -1) {
                return;
            }
            if (!Config.enable_tls
                    || !TlsProtocolSet.isProtocolIncluded(TlsProtocolSet.Protocol.ARROWFLIGHT)) {
                LOG.info("TLS: Arrow Flight excluded or enable_tls=false — starting plain on port {}", port);
                startPlain();
                return;
            }

            // Arrow Flight TLS uses Location.forGrpcTls + NettyServerCredentials
            // This requires the arrow-flight-core gRPC TLS integration.
            // TODO: integrate with TlsContextFactory for full Arrow Flight TLS.
            LOG.warn("TLS: Arrow Flight TLS not yet fully implemented — starting plain. "
                    + "Add 'arrowflight' to tls_excluded_protocols to suppress this warning.");
            startPlain();
        }

        private void startPlain() {
            flightSqlService = new DorisFlightSqlService(port);
            if (!flightSqlService.start()) {
                throw new IllegalStateException("Arrow Flight SQL server failed to start");
            }
        }

        @Override
        public void stop() {
            if (flightSqlService != null) {
                flightSqlService.stop();
            }
        }
    }
}
