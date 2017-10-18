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
package com.facebook.presto.server;

import com.facebook.presto.client.CreateTokenResponse;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.security.LoginTokenStore;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.Identity;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import java.security.Principal;
import java.util.Optional;

@Path("/v1/token")
public class LoginTokenResource
{
    private final LoginTokenStore tokenStore;
    private final AccessControl accessControl;

    @Inject
    public LoginTokenResource(LoginTokenStore tokenStore, AccessControl accessControl)
    {
        this.tokenStore = tokenStore;
        this.accessControl = accessControl;
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public Response createToken(@Context HttpServletRequest servletRequest)
    {
        SessionSupplier sessionSupplier = new HttpRequestSessionFactory(servletRequest, ImmutableMap.of());
        Identity identity = sessionSupplier.getIdentity();

        Optional<Principal> maybePrincipal = identity.getPrincipal();
        if (!maybePrincipal.isPresent()) {
            return Response.status(Status.FORBIDDEN).build();
        }
        Principal principal = maybePrincipal.get();
        String user = identity.getUser();
        try {
            accessControl.checkCanSetUser(principal, user);
        }
        catch (AccessDeniedException e) {
            return Response.status(Status.FORBIDDEN).build();
        }

        String token = tokenStore.createToken(identity.getUser(), principal);
        return Response.ok(new CreateTokenResponse(token)).build();
    }
}
