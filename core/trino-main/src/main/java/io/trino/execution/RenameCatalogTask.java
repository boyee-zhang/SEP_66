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
package io.trino.execution;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.CatalogManager;
import io.trino.security.AccessControl;
import io.trino.spi.catalog.CatalogName;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.RenameCatalog;

import java.util.List;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static java.util.Objects.requireNonNull;

public class RenameCatalogTask
        implements DataDefinitionTask<RenameCatalog>
{
    private final CatalogManager catalogManager;
    private final AccessControl accessControl;

    @Inject
    public RenameCatalogTask(CatalogManager catalogManager, AccessControl accessControl)
    {
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public String getName()
    {
        return "RENAME CATALOG";
    }

    @Override
    public ListenableFuture<Void> execute(
            RenameCatalog statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();

        accessControl.checkCanRenameCatalog(session.toSecurityContext(), statement.getSource().getValue(), statement.getTarget().getValue());

        catalogManager.renameCatalog(new CatalogName(statement.getSource().getValue()), new CatalogName(statement.getTarget().getValue()));
        return immediateVoidFuture();
    }
}
