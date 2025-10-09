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

package org.apache.doris.mysql;

import org.apache.doris.common.Config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Enhanced MySQL protocol handshake packet with dynamic authentication plugin selection.
 *
 * This enhanced handshake packet supports multiple authentication plugins:
 * - caching_sha2_password (MySQL 8.0+ default, secure SHA-256 based)
 * - mysql_native_password (legacy SHA-1 based, for compatibility)
 *
 * The packet dynamically selects the authentication plugin based on configuration
 * and client capabilities, enabling secure authentication while maintaining
 * backward compatibility.
 */
public class MysqlEnhancedHandshakePacket extends MysqlHandshakePacket {
    private static final Logger LOG = LogManager.getLogger(MysqlEnhancedHandshakePacket.class);

    // Authentication plugin names
    public static final String CACHING_SHA2_PASSWORD_PLUGIN = "caching_sha2_password";
    public static final String MYSQL_NATIVE_PASSWORD_PLUGIN = "mysql_native_password";

    // Enhanced capabilities for caching_sha2_password
    private static final MysqlCapability ENHANCED_CAPABILITY = new MysqlCapability(
            MysqlCapability.DEFAULT_CAPABILITY.getFlags()
            | MysqlCapability.Flag.CLIENT_PLUGIN_AUTH.getFlagBit()
            | MysqlCapability.Flag.CLIENT_SECURE_CONNECTION.getFlagBit()
    );

    private static final MysqlCapability ENHANCED_SSL_CAPABILITY = new MysqlCapability(
            MysqlCapability.SSL_CAPABILITY.getFlags()
            | MysqlCapability.Flag.CLIENT_PLUGIN_AUTH.getFlagBit()
            | MysqlCapability.Flag.CLIENT_SECURE_CONNECTION.getFlagBit()
    );

    private final String selectedAuthPlugin;
    private final boolean useEnhancedAuth;

    /**
     * Constructor with dynamic authentication plugin selection
     */
    public MysqlEnhancedHandshakePacket(int connectionId) {
        super(connectionId);
        // Determine authentication plugin based on configuration
        this.useEnhancedAuth = Config.enable_caching_sha2_password;
        this.selectedAuthPlugin = useEnhancedAuth
            ? CACHING_SHA2_PASSWORD_PLUGIN : MYSQL_NATIVE_PASSWORD_PLUGIN;


        if (LOG.isDebugEnabled()) {
            LOG.debug("Enhanced handshake packet created with auth plugin: {}", selectedAuthPlugin);
        }
    }

    /**
     * Constructor with explicit authentication plugin selection
     */
    public MysqlEnhancedHandshakePacket(int connectionId, String authPlugin) {
        super(connectionId);

        // Validate and set authentication plugin
        if (CACHING_SHA2_PASSWORD_PLUGIN.equals(authPlugin)
                || MYSQL_NATIVE_PASSWORD_PLUGIN.equals(authPlugin)) {
            this.selectedAuthPlugin = authPlugin;
            this.useEnhancedAuth = CACHING_SHA2_PASSWORD_PLUGIN.equals(authPlugin);
        } else {
            LOG.warn("Unknown authentication plugin '{}', falling back to mysql_native_password", authPlugin);
            this.selectedAuthPlugin = MYSQL_NATIVE_PASSWORD_PLUGIN;
            this.useEnhancedAuth = false;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Enhanced handshake packet created with explicit auth plugin: {}", selectedAuthPlugin);
        }
    }

    /**
     * Get the selected authentication plugin name
     */
    public String getSelectedAuthPlugin() {
        return selectedAuthPlugin;
    }

    /**
     * Check if enhanced authentication (caching_sha2_password) is enabled
     */
    public boolean isEnhancedAuthEnabled() {
        return useEnhancedAuth;
    }

    @Override
    public void writeTo(MysqlSerializer serializer) {
        // Select capability based on SSL and enhanced auth settings
        MysqlCapability capability;
        if (MysqlProto.SERVER_USE_SSL) {
            capability = useEnhancedAuth ? ENHANCED_SSL_CAPABILITY : MysqlCapability.SSL_CAPABILITY;
        } else {
            capability = useEnhancedAuth ? ENHANCED_CAPABILITY : MysqlCapability.DEFAULT_CAPABILITY;
        }

        // Write protocol version
        serializer.writeInt1(getProtocolVersion());

        // Write server version
        serializer.writeNulTerminateString(getServerVersion());

        // Write connection ID
        serializer.writeInt4(getConnectionId());

        // Write first 8 bytes of auth plugin data (nonce/salt)
        byte[] authPluginData = getAuthPluginData();
        serializer.writeBytes(authPluginData, 0, 8);

        // Write filler
        serializer.writeInt1(0);

        // Write lower 2 bytes of capability flags
        serializer.writeInt2(capability.getFlags() & 0XFFFF);

        // Write character set
        serializer.writeInt1(getCharacterSet());

        // Write status flags
        serializer.writeInt2(getStatusFlags());

        // Write upper 2 bytes of capability flags
        serializer.writeInt2(capability.getFlags() >> 16);

        // Write auth plugin data length if plugin auth is supported
        if (capability.isPluginAuth()) {
            serializer.writeInt1(authPluginData.length + 1); // +1 for null terminator
        } else {
            serializer.writeInt1(0);
        }

        // Write reserved 10 zeros
        serializer.writeBytes(new byte[10]);

        // Write remaining auth plugin data if secure connection is supported
        if (capability.isSecureConnection()) {
            // MySQL protocol requires writing at least 13 bytes here
            // Write max(13, len(auth-plugin-data) - 8) bytes
            serializer.writeBytes(authPluginData, 8, 12);
            // Append one byte to reach 13 bytes minimum
            serializer.writeInt1(0);
        }

        // Write authentication plugin name if plugin auth is supported
        if (capability.isPluginAuth()) {
            serializer.writeNulTerminateString(selectedAuthPlugin);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Enhanced handshake packet written with plugin: {}, SSL: {}, capabilities: 0x{}",
                    selectedAuthPlugin, MysqlProto.SERVER_USE_SSL,
                    Integer.toHexString(capability.getFlags()));
        }
    }

    @Override
    public boolean checkAuthPluginSameAsDoris(String pluginName) {
        boolean isSame = selectedAuthPlugin.equals(pluginName);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Auth plugin check: client='{}', server='{}', same={}",
                    pluginName, selectedAuthPlugin, isSame);
        }

        return isSame;
    }

    @Override
    public void buildAuthSwitchRequest(MysqlSerializer serializer) {
        // Build authentication switch request packet
        serializer.writeInt1((byte) 0xfe); // AUTH_SWITCH_REQUEST
        serializer.writeNulTerminateString(selectedAuthPlugin);
        serializer.writeBytes(getAuthPluginData());
        serializer.writeInt1(0); // null terminator
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("Auth switch request built for plugin: {}", selectedAuthPlugin);
        }
    }

    /**
     * Check if the client supports caching_sha2_password
     */
    public boolean clientSupportsCachingSha2Password(MysqlCapability clientCapability) {
        // Check if client supports plugin authentication and secure connection
        boolean supportsPluginAuth = clientCapability.isPluginAuth();
        boolean supportsSecureConnection = clientCapability.isSecureConnection();

        boolean supports = supportsPluginAuth && supportsSecureConnection;

        if (LOG.isDebugEnabled()) {
            LOG.debug("Client caching_sha2_password support check: pluginAuth={}, secureConnection={}, result={}",
                    supportsPluginAuth, supportsSecureConnection, supports);
        }

        return supports;
    }

    /**
     * Determine the best authentication plugin for the client
     */
    public String determineBestAuthPlugin(MysqlCapability clientCapability) {
        // If enhanced auth is disabled, use mysql_native_password
        if (!useEnhancedAuth) {
            return MYSQL_NATIVE_PASSWORD_PLUGIN;
        }

        // If client supports caching_sha2_password, use it
        if (clientSupportsCachingSha2Password(clientCapability)) {
            return CACHING_SHA2_PASSWORD_PLUGIN;
        }

        // Fall back to mysql_native_password for compatibility
        LOG.info("Client does not support caching_sha2_password, falling back to mysql_native_password");
        return MYSQL_NATIVE_PASSWORD_PLUGIN;
    }

    // Protected getters for accessing parent class fields
    protected int getProtocolVersion() {
        return 10; // Protocol version 10
    }

    protected String getServerVersion() {
        return "5.7.99"; // Server version string
    }

    protected int getConnectionId() {
        return super.hashCode(); // Use object hash as connection ID placeholder
    }

    protected int getCharacterSet() {
        return 33; // UTF-8 character set
    }

    protected int getStatusFlags() {
        return 0; // No status flags
    }


    public String getStatistics() {
        return String.format("MysqlEnhancedHandshakePacket[plugin=%s, enhanced=%s, ssl=%s]",
                selectedAuthPlugin, useEnhancedAuth, MysqlProto.SERVER_USE_SSL);
    }
}
