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

import java.util.Arrays;

public class NativePassword implements Password {
    private byte[] remotePasswd;
    private byte[] randomString;
    private boolean authenticated = true;

    public NativePassword(byte[] remotePasswd, byte[] randomString) {
        this.remotePasswd = remotePasswd;
        this.randomString = randomString;
    }

    public byte[] getRemotePasswd() {
        return remotePasswd;
    }

    @Override
    public String getAuthPluginName() {
        return "mysql_native_password";
    }

    @Override
    public boolean isAuthenticated() {
        return authenticated;
    }

    @Override
    public String getPlainTextPassword() {
        return null; // Native password doesn't have plain text
    }

    @Override
    public byte[] getScrambledPassword() {
        return remotePasswd;
    }

    @Override
    public byte[] getNonce() {
        return randomString;
    }

    @Override
    public void clearPassword() {
        if (remotePasswd != null) {
            Arrays.fill(remotePasswd, (byte) 0);
        }
    }

    @Override
    public String toSafeString() {
        return "NativePassword{authenticated=" + authenticated + "}";
    }
}
