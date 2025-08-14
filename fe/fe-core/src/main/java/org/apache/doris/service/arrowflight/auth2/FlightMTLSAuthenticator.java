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

package org.apache.doris.service.arrowflight.auth2;

import org.apache.doris.service.arrowflight.tokens.FlightTokenManager;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.netty.handler.ssl.SslHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.cert.X509Certificate;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

/**
 * Doris's implementation of CallHeaderAuthenticator for mTLS authentication.
 * This authenticator extracts client certificates from gRPC calls and maps them to usernames
 * using the same logic as MTLSUtils.
 */
public class FlightMTLSAuthenticator implements CallHeaderAuthenticator {
    private static final Logger LOG = LogManager.getLogger(FlightMTLSAuthenticator.class);
    private static final Context.Key<X509Certificate[]> CLIENT_CERTIFICATES = Context.key("client-certificates");

    private final FlightTokenManager flightTokenManager;

    public FlightMTLSAuthenticator(FlightTokenManager flightTokenManager) {
        this.flightTokenManager = flightTokenManager;
    }

    /**
     * Authenticates the client using the client certificate.
     *
     * @param incomingHeaders call headers
     * @return an AuthResult with the username derived from the certificate
     */
    @Override
    public AuthResult authenticate(CallHeaders incomingHeaders) {
        // Get client certificates from gRPC context
        X509Certificate[] certs = CLIENT_CERTIFICATES.get();
        if (certs == null || certs.length == 0) {
            LOG.warn("No client certificate found for mTLS authentication");
            throw CallStatus.UNAUTHENTICATED.withDescription("No client certificate provided").toRuntimeException();
        }

        X509Certificate clientCert = certs[0];

        try {
            // Use MTLSFlightUtils to validate certificate and get auth result
            FlightAuthResult flightAuthResult =
                MTLSFlightUtils.validateCertificateAndGetAuthResult(clientCert, "0.0.0.0");

            // Create token for the user
            String token = FlightAuthUtils.createToken(
                flightTokenManager, flightAuthResult.getUserName(), flightAuthResult);

            // Return AuthResult with token
            return createAuthResultWithBearerToken(token);
        } catch (Exception e) {
            LOG.error("Failed to authenticate with mTLS", e);
            throw CallStatus.UNAUTHENTICATED.withCause(e).withDescription(e.getMessage()).toRuntimeException();
        }
    }

    /**
     * Helper method to create an AuthResult with bearer token.
     *
     * @param token the token
     * @return an AuthResult with the token
     */
    private AuthResult createAuthResultWithBearerToken(String token) {
        return new AuthResult() {
            @Override
            public void appendToOutgoingHeaders(CallHeaders outgoingHeaders) {
                // No headers needed for mTLS
            }

            @Override
            public String getPeerIdentity() {
                return token;
            }
        };
    }

    /**
     * Creates a gRPC interceptor that extracts client certificates from SSL sessions
     * and stores them in the gRPC context.
     *
     * @return a gRPC interceptor
     */
    public static ServerInterceptor createCertificateInterceptor() {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call,
                    Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {

                try {
                    // Extract SSL session from gRPC call
                    SSLSession sslSession = call.getAttributes().get(SslHandler.class.getName());
                    if (sslSession != null) {
                        try {
                            // Get client certificates from SSL session
                            X509Certificate[] certs = (X509Certificate[]) sslSession.getPeerCertificates();
                            // Store certificates in context
                            Context context = Context.current().withValue(CLIENT_CERTIFICATES, certs);
                            return Contexts.interceptCall(context, call, headers, next);
                        } catch (SSLPeerUnverifiedException e) {
                            LOG.warn("Failed to get peer certificates", e);
                        }
                    }
                } catch (Exception e) {
                    LOG.warn("Failed to extract SSL session", e);
                }

                // Continue without certificates
                return next.startCall(call, headers);
            }
        };
    }
}
