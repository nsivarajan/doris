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

package org.apache.doris.mysql.authenticate;

import org.apache.doris.mysql.authenticate.Authenticator;

import java.util.Properties;

/**
 * Factory interface for creating authenticators.
 * This interface is used with Java's ServiceLoader mechanism to dynamically load
 * authenticator implementations.
 */
public interface AuthenticatorFactory {
    /**
     * Creates an authenticator instance with the given initialization properties.
     *
     * @param initProps Properties to initialize the authenticator with
     * @return An authenticator instance
     */
    Authenticator create(Properties initProps);

    /**
     * Returns a unique identifier for this factory.
     * This identifier is used to select the appropriate authenticator factory.
     *
     * @return A string identifier for this factory
     */
    String factoryIdentifier();
}
