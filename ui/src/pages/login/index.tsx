/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
 
import React, {useEffect} from 'react';
import {Form, Input, Button, message} from 'antd';
import request from 'Utils/request';
import {useHistory} from 'react-router-dom';
import {useTranslation} from 'react-i18next';
import {login, getAuthInfo} from 'Src/api/api';
import styles from './index.less';
import './cover.less';

function Login(){
    const { t } = useTranslation();
    const history = useHistory();
    const layout = {
        labelCol: {span: 8},
        wrapperCol: {span: 24},
        layout: 'vertical',
    };
    const tailLayout = {
        wrapperCol: {span: 24},
    };

    interface Result<T> {
        content: T;
        status: string;
        partialResults: boolean;
        msg: string;
        code: number;
    }
    
    // Define interface for auth info response
    interface AuthInfoResponse {
        authenticated?: boolean;
        username?: string;
        authType?: string;
        code?: number;
    }
    const onFinish = values => {
        login(values).then((res: any) => {
            if(res.code === 200){
                const usernameToStore = res.displayUsername || values.username;
                localStorage.setItem('username', usernameToStore);
                history.push('/home');
            } else {
                // Show error message if login fails
                message.error(t('loginWarning'));
            }
        }).catch(err => {
            message.error(t('errMsg'));
        });
    };

    const onFinishFailed = errorInfo => {
        console.log('Failed:', errorInfo);
    };
    // Check if MTLS authentication is enabled and try to authenticate
    useEffect(() => {
        // Use our new auth_info endpoint that handles client certificates properly
        getAuthInfo()
            .then((res: AuthInfoResponse) => {
                if (res && res.code === 200 && res.authenticated === true && res.username) {
                    // MTLS authentication successful, store username and redirect to home
                    localStorage.setItem('username', res.username);
                    history.push('/home');
                }
            })
            .catch(err => {
                // MTLS authentication failed or not enabled - just continue to show login form
                console.log('Auth info check failed or not enabled');
            });
    }, []);

    // 878CB1
    // 31395B
    return (
        <div className={[styles['background'],'login'].join(' ')}>
            <div className={styles['logo']}></div>
            <Form
                {...layout}
                name="basic"
                initialValues={{remember: true}}
                onFinish={onFinish}
                layout='vertical'
                requiredMark={false}
                onFinishFailed={onFinishFailed}
                className={styles['login-form']}
            >
                <Form.Item
                    label={t('username')}
                    name="username"
                    rules={[{required: true, message: 'Please input your username!'}]}
                >
                    <Input autoComplete="username" />
                </Form.Item>

                <Form.Item
                    label={t('password')}
                    name="password"
                    rules={[{required: false, message: 'Please input your password!'}]}
                >
                    <Input.Password autoComplete="current-password" />
                </Form.Item>

                <Form.Item {...tailLayout}>
                    <Button type="primary" shape="round" style={{'width':'100%'}} htmlType="submit">
                        {t('login')}
                    </Button>
                </Form.Item>
            </Form>
        </div>
    );
}

export default Login;
 