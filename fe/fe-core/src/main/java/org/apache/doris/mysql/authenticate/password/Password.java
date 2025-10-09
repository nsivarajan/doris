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

package org.apache.doris.mysql.authenticate.password;

/**
 * Base interface for password authentication data structures.
 *
 * This interface represents the result of password authentication processing,
 * containing the necessary information for user authentication and authorization.
 * Different authentication plugins may implement this interface to provide
 * plugin-specific password handling and validation.
 *
 * Implementations should be immutable once authentication is complete to ensure
 * thread safety and prevent tampering with authentication results.
 */
public interface Password {
    
    /**
     * Get the authentication plugin name that processed this password.
     *
     * @return The name of the authentication plugin (e.g., "caching_sha2_password", "mysql_native_password")
     */
    String getAuthPluginName();
    
    /**
     * Check if the password authentication was successful.
     *
     * @return true if authentication succeeded, false otherwise
     */
    boolean isAuthenticated();
    
    /**
     * Get the plain text password if available.
     *
     * Note: This method should only return the password if it was securely obtained
     * through SSL or RSA encryption. Implementations should clear the password
     * from memory as soon as it's no longer needed for security reasons.
     *
     * @return The plain text password, or null if not available or already cleared
     */
    String getPlainTextPassword();
    
    /**
     * Get the scrambled/hashed password data.
     *
     * @return The scrambled password bytes, or null if not available
     */
    byte[] getScrambledPassword();
    
    /**
     * Get the nonce/salt used for password scrambling.
     *
     * @return The nonce bytes used in authentication, or null if not applicable
     */
    byte[] getNonce();
    
    /**
     * Clear sensitive password data from memory.
     *
     * This method should be called after authentication is complete to ensure
     * that sensitive password information is not left in memory longer than necessary.
     * After calling this method, getPlainTextPassword() should return null.
     */
    void clearPassword();
    
    /**
     * Get a string representation suitable for logging (without sensitive data).
     *
     * @return A safe string representation that doesn't expose password data
     */
    String toSafeString();
}
