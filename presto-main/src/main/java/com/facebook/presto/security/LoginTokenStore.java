/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.security;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.security.Principal;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class LoginTokenStore
{
    private final Cache<String, LoginToken> cache = CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.HOURS).build();

    public String createToken(final String userName, final Principal principal)
    {
        String token = UUID.randomUUID().toString();
        cache.put(token, new LoginToken(token, userName, principal));
        return token;
    }

    public Optional<String> getUser(final String token)
    {
        LoginToken loginToken = cache.getIfPresent(token);
        return Optional.ofNullable(loginToken).map(LoginToken::getUser);
    }

    public Optional<Principal> getPrincipal(final String token)
    {
        LoginToken loginToken = cache.getIfPresent(token);
        return Optional.ofNullable(loginToken).map(LoginToken::getPrincipal);
    }
}

class LoginToken
{
    private final String token;
    private final String user;
    private final Principal principal;

    LoginToken(String token, String user, Principal principal)
    {
        this.token = token;
        this.user = user;
        this.principal = principal;
    }

    String getToken()
    {
        return token;
    }

    String getUser()
    {
        return user;
    }

    Principal getPrincipal()
    {
        return principal;
    }
}
