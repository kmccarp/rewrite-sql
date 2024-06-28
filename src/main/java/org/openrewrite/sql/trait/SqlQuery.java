/*
 * Copyright 2024 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.openrewrite.sql.trait;

import fj.data.Validation;
import lombok.AllArgsConstructor;
import lombok.Getter;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;
import net.sf.jsqlparser.util.deparser.StatementDeParser;
import org.openrewrite.Cursor;
import org.openrewrite.Tree;
import org.openrewrite.analysis.trait.Top;
import org.openrewrite.analysis.trait.TraitFactory;
import org.openrewrite.analysis.trait.util.TraitErrors;
import org.openrewrite.internal.lang.Nullable;
import org.openrewrite.java.tree.Expression;
import org.openrewrite.java.tree.J;
import org.openrewrite.sql.internal.ChangeTrackingExpressionDeParser;
import org.openrewrite.text.PlainText;

import java.util.UUID;
import java.util.regex.Pattern;

public interface SqlQuery extends Top {
    enum Factory implements TraitFactory<SqlQuery> {
        F;

        @Override
        public Validation<TraitErrors, SqlQuery> viewOf(Cursor cursor) {
            if (cursor.getValue() instanceof J.Literal) {
                J.Literal literal = cursor.getValue();
                if (SqlDetector.probablySql(literal.getValue())) {
                    return SqlQueryBase.viewOf(cursor, literal.getValue().toString()).map(m -> m);
                }
            } else if (cursor.getValue() instanceof PlainText) {
                PlainText plainText = cursor.getValue();
                if (SqlDetector.probablySql(plainText.getText())) {
                    return SqlQueryBase.viewOf(cursor, plainText.getText()).map(m -> m);
                }
            }
            return TraitErrors.invalidTraitCreationType(SqlQuery.class, cursor,
                    Expression.class, PlainText.class);
        }
    }

    static Validation<TraitErrors, SqlQuery> viewOf(Cursor cursor) {
        return SqlQuery.Factory.F.viewOf(cursor);
    }

    Tree mapSql(ExpressionDeParser map);

    String getSql();

    Statement getQuery();
}

class SqlDetector {
    private static final Pattern SIMPLE_SQL_HEURISTIC = Pattern.compile("SELECT|UPDATE|DELETE|INSERT",
            Pattern.CASE_INSENSITIVE);

    static boolean probablySql(@Nullable Object maybeSql) {
        return maybeSql != null && SIMPLE_SQL_HEURISTIC.matcher(maybeSql.toString()).find();
    }
}

@AllArgsConstructor
class SqlQueryBase extends Top.Base implements SqlQuery {
    private final Tree tree;

    @Getter
    private final String sql;

    @Getter
    private final Statement query;

    static Validation<TraitErrors, SqlQueryBase> viewOf(Cursor cursor, String sql) {
        try {
            return Validation.success(new SqlQueryBase(cursor.getValue(), sql, CCJSqlParserUtil.parse(sql)));
        } catch (JSQLParserException e) {
            return TraitErrors.invalidTraitCreation(SqlQuery.class,
                    "Failed to parse SQL: " + e.getMessage());
        }
    }

    @Override
    public Tree mapSql(ExpressionDeParser map) {
        try {
            StringBuilder sb = new StringBuilder();

            SelectDeParser selectDeParser = new SelectDeParser(map, sb);
            map.setSelectVisitor(selectDeParser);
            map.setBuffer(sb);
            StatementDeParser statementDeParser = new StatementDeParser(map, selectDeParser, sb);

            query.accept(statementDeParser);
            return updateSql(sb.toString(), map);
        } catch (Throwable t) {
            // this is invalid sql
            return tree;
        }
    }

    private Tree updateSql(String sql, ExpressionDeParser deparser) {
        if (deparser instanceof ChangeTrackingExpressionDeParser) {
            sql = ChangeTrackingExpressionDeParser.applyChange(this.sql, sql);
        }
        if (tree instanceof PlainText text) {
            return text.withText(sql);
        } else if (tree instanceof J.Literal literal) {
            return literal.withValue(sql)
                    .withValueSource("\"" + sql + "\"");
        }
        return tree;
    }

    @Override
    public UUID getId() {
        return tree.getId();
    }
}
