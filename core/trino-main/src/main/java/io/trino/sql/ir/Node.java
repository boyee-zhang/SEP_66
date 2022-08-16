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
package io.trino.sql.ir;

import java.util.List;

public abstract class Node
{
    protected Node()
    {
    }

    /**
     * Accessible for {@link IrVisitor}, use {@link IrVisitor#process(Node, Object)} instead.
     */
    protected <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitNode(this, context);
    }

    public abstract List<? extends Node> getChildren();

    // Force subclasses to have a proper equals and hashcode implementation
    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object obj);

    @Override
    public abstract String toString();

    /**
     * Compare with another node by considering internal state excluding any Node returned by getChildren()
     */
    public boolean shallowEquals(Node other)
    {
        throw new UnsupportedOperationException("not yet implemented: " + getClass().getName());
    }

    static boolean sameClass(Node left, Node right)
    {
        if (left == right) {
            return true;
        }

        return left.getClass() == right.getClass();
    }
}
