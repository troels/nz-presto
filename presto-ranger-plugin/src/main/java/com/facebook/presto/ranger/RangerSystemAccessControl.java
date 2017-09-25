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
package com.facebook.presto.ranger;

import com.facebook.presto.spi.CatalogSchemaName;
import com.facebook.presto.spi.CatalogSchemaTableName;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.SystemAccessControl;
import io.airlift.log.Logger;

import java.security.Principal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.security.AccessDeniedException.denyAddColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateSchema;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateView;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateViewWithSelect;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDeleteTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropSchema;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropView;
import static com.facebook.presto.spi.security.AccessDeniedException.denyInsertTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameSchema;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denySelectTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denySelectView;
import static com.facebook.presto.spi.security.AccessDeniedException.denySetSystemSessionProperty;
import static com.facebook.presto.spi.security.AccessDeniedException.denySetUser;
import static com.facebook.presto.spi.security.AccessDeniedException.denyShowTablesMetadata;

public class RangerSystemAccessControl
        implements SystemAccessControl
{
    private static final Logger log = Logger.get(RangerSystemAccessControl.class);
    private final Set<String> powerPrincipals;

    private final Set<String> powerUsers;
    private final PrestoAuthorizer authorizer;
    private final Set<String> writeableCatalogs;

    public RangerSystemAccessControl(PrestoAuthorizer prestoAuthorizer, Map<String, String> config)
    {
        this.authorizer = prestoAuthorizer;

        String[] writeableCatalogs = config.getOrDefault("writeable-catalogs", "").split(",");
        this.writeableCatalogs = Arrays.stream(writeableCatalogs).filter(s -> !s.isEmpty()).collect(Collectors.toSet());

        log.info("Writeable catalogs: " + this.writeableCatalogs);
        String[] powerPrincipals = config.getOrDefault("power-principals", "")
                .split(",");
        this.powerPrincipals = Arrays.stream(powerPrincipals)
                .filter(s -> !s.isEmpty())
                .map(String::toLowerCase)
                .collect(Collectors.toSet());

        String[] powerUsers = config.getOrDefault("power-users", "")
                .split(",");
        this.powerUsers = Arrays.stream(powerUsers)
                .filter(s -> !s.isEmpty())
                .map(String::toLowerCase)
                .collect(Collectors.toSet());
    }

    @Override
    public void checkCanSetUser(Principal principal, String userName)
    {
        if (principal == null) {
            return;
        }
        if (powerPrincipals.contains(principal.getName().toLowerCase())) {
            return;
        }
        String principalName = principal.getName()
                .replaceAll("@.*", "")
                .replaceAll("/.*", "");
        if (!principalName.equalsIgnoreCase(userName)) {
            denySetUser(principal, userName);
        }
    }

    @Override
    public Set<String> filterCatalogs(Identity identity, Set<String> catalogs)
    {
        return catalogs;
    }

    @Override
    public void checkCanSelectFromView(Identity identity, CatalogSchemaTableName view)
    {
        if (view.getSchemaTableName().getSchemaName().equalsIgnoreCase("information_schema")) {
            return;
        }

        if (!authorizer.canSelectOnResource(createResource(view), identity)) {
            denySelectView(view.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanSelectFromTable(Identity identity, CatalogSchemaTableName table)
    {
        if (table.getSchemaTableName().getSchemaName().equalsIgnoreCase("information_schema")) {
            return;
        }

        if (!authorizer.canSelectOnResource(createResource(table), identity)) {
            denySelectTable(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
        if (!powerUsers.contains(identity.getUser().toLowerCase())) {
            denySetSystemSessionProperty(propertyName);
        }
    }

    @Override
    public void checkCanSetCatalogSessionProperty(Identity identity, String catalogName, String propertyName)
    {
        if (!powerUsers.contains(identity.getUser().toLowerCase())) {
            denySetSystemSessionProperty(propertyName);
        }
    }

    @Override
    public void checkCanAccessCatalog(Identity identity, String catalogName)
    {
    }

    @Override
    public void checkCanShowSchemas(Identity identity, String catalogName)
    {
    }

    @Override
    public void checkCanShowTablesMetadata(Identity identity, CatalogSchemaName schema)
    {
        if (!authorizer.canSeeResource(createResource(schema), identity)) {
            denyShowTablesMetadata(schema.getSchemaName());
        }
    }

    @Override
    public Set<SchemaTableName> filterTables(Identity identity, String catalogName, Set<SchemaTableName> tableNames)
    {
        List<RangerPrestoResource> rangerResources = tableNames
                .stream()
                .map(t -> new RangerPrestoResource(t.getSchemaName(), Optional.of(t.getTableName())))
                .collect(Collectors.toList());

        return authorizer
                .filterResources(rangerResources, identity)
                .stream()
                .map(RangerPrestoResource::getSchemaTable)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<String> filterSchemas(Identity identity, String catalogName, Set<String> schemaNames)
    {
        List<RangerPrestoResource> rangerResources = schemaNames
                .stream()
                .map(schemaName -> new RangerPrestoResource(schemaName, Optional.empty()))
                .collect(Collectors.toList());

        return authorizer
                .filterResources(rangerResources, identity)
                .stream()
                .map(RangerPrestoResource::getDatabase)
                .collect(Collectors.toSet());
    }

    @Override
    public void checkCanCreateSchema(Identity identity, CatalogSchemaName schema)
    {
        if (!authorizer.canCreateResource(createResource(schema), identity) ||
                !writeableCatalogs.contains(schema.getCatalogName())) {
            denyCreateSchema(schema.getSchemaName());
        }
    }

    @Override
    public void checkCanDropSchema(Identity identity, CatalogSchemaName schema)
    {
        if (!authorizer.canCreateResource(createResource(schema), identity) ||
                !writeableCatalogs.contains(schema.getCatalogName())) {
            denyDropSchema(schema.getSchemaName());
        }
    }

    @Override
    public void checkCanRenameSchema(Identity identity, CatalogSchemaName schema, String newSchemaName)
    {
        if (!authorizer.canCreateResource(createResource(schema), identity) ||
                !authorizer.canDropResource(createResource(newSchemaName), identity) ||
                !writeableCatalogs.contains(schema.getCatalogName())) {
            denyRenameSchema(schema.getSchemaName(), newSchemaName);
        }
    }

    @Override
    public void checkCanCreateTable(Identity identity, CatalogSchemaTableName table)
    {
        if (!authorizer.canCreateResource(createResource(table), identity) ||
                !writeableCatalogs.contains(table.getCatalogName())) {
            denyCreateTable(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanDropTable(Identity identity, CatalogSchemaTableName table)
    {
        if (!authorizer.canDropResource(createResource(table), identity) ||
                !writeableCatalogs.contains(table.getCatalogName())) {
            denyDropTable(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanRenameTable(Identity identity, CatalogSchemaTableName table, CatalogSchemaTableName newTable)
    {
        if (!authorizer.canCreateResource(createResource(newTable), identity) ||
                !authorizer.canDropResource(createResource(table), identity) ||
                !writeableCatalogs.contains(newTable.getCatalogName()) ||
                !writeableCatalogs.contains(table.getCatalogName())) {
            denyRenameTable(table.getSchemaTableName().getTableName(), newTable.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanAddColumn(Identity identity, CatalogSchemaTableName table)
    {
        if (!authorizer.canUpdateResource(createResource(table), identity) ||
                !writeableCatalogs.contains(table.getCatalogName())) {
            denyAddColumn(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanDropColumn(Identity identity, CatalogSchemaTableName table)
    {
        if (!authorizer.canUpdateResource(createResource(table), identity) ||
                !writeableCatalogs.contains(table.getCatalogName())) {
            denyDropColumn(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanRenameColumn(Identity identity, CatalogSchemaTableName table)
    {
        if (!authorizer.canUpdateResource(createResource(table), identity) ||
                !writeableCatalogs.contains(table.getCatalogName())) {
            denyRenameColumn(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanInsertIntoTable(Identity identity, CatalogSchemaTableName table)
    {
        if (!authorizer.canUpdateResource(createResource(table), identity) ||
                !writeableCatalogs.contains(table.getCatalogName())) {
            denyInsertTable(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanDeleteFromTable(Identity identity, CatalogSchemaTableName table)
    {
        if (!authorizer.canUpdateResource(createResource(table), identity) ||
                !writeableCatalogs.contains(table.getCatalogName())) {
            denyDeleteTable(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanCreateView(Identity identity, CatalogSchemaTableName view)
    {
        if (!authorizer.canCreateResource(createResource(view), identity) ||
                !writeableCatalogs.contains(view.getCatalogName())) {
            denyCreateView(view.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanDropView(Identity identity, CatalogSchemaTableName view)
    {
        if (!authorizer.canDropResource(createResource(view), identity) ||
                !writeableCatalogs.contains(view.getCatalogName())) {
            denyDropView(view.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromTable(Identity identity, CatalogSchemaTableName table)
    {
        if (table.getSchemaTableName().getSchemaName().equalsIgnoreCase("information_schema")) {
            return;
        }

        if (!authorizer.canSelectOnResource(createResource(table), identity)) {
            denyCreateViewWithSelect(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromView(Identity identity, CatalogSchemaTableName view)
    {
        checkCanCreateViewWithSelectFromView(identity, view);
    }

    private static RangerPrestoResource createResource(CatalogSchemaName catalogSchema)
    {
        return createResource(catalogSchema.getSchemaName());
    }

    private static RangerPrestoResource createResource(CatalogSchemaTableName catalogSchema)
    {
        return createResource(catalogSchema.getSchemaTableName().getSchemaName(), catalogSchema.getSchemaTableName().getTableName());
    }

    private static RangerPrestoResource createResource(SchemaTableName tableName)
    {
        return createResource(tableName.getSchemaName(), tableName.getTableName());
    }

    private static RangerPrestoResource createResource(final String schemaName)
    {
        return new RangerPrestoResource(schemaName, Optional.empty());
    }

    private static RangerPrestoResource createResource(final String schemaName, final String tableName)
    {
        return new RangerPrestoResource(schemaName, Optional.of(tableName));
    }
}
