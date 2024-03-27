/*
 * Copyright 2023 the original author or authors.
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
package org.openrewrite.sql;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.openrewrite.Tree;
import org.openrewrite.marker.GitProvenance;
import org.openrewrite.sql.table.DatabaseColumnsUsed;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.openrewrite.java.Assertions.java;
import static org.openrewrite.test.SourceSpecs.text;
import static org.openrewrite.yaml.Assertions.yaml;

@SuppressWarnings({"SqlResolve", "SqlNoDataSourceInspection"})
class FindSqlTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new FindSql());
    }

    @ParameterizedTest
    @ValueSource(strings = {
      "This will be SELECTed by the heuristic but not parse as SQL",
      "The heuristic won't match this at all"
    })
    void notSql(String maybeSql) {
        rewriteRun(
          text(maybeSql)
        );
    }

    @Test
    void select() {
        //noinspection SqlDialectInspection
        rewriteRun(
          spec -> spec.dataTable(DatabaseColumnsUsed.Row.class, rows -> {
              assertThat(rows).hasSize(1);
              DatabaseColumnsUsed.Row row = rows.get(0);
              assertThat(row.getOperation()).isEqualTo(DatabaseColumnsUsed.Operation.SELECT);
              assertThat(row.getTable()).isEqualTo("users");
              assertThat(row.getColumn()).isEqualTo("first_name");
              assertThat(row.getGetCommitHash()).isEqualTo("1234");
          }),
          text(
            // language=sql
            """
              SELECT distinct(first_name)
              FROM users
              WHERE user_id = :userId
              AND :user_type = ANY (user_types)
              """,
            spec -> spec
              .path("select.sql")
              .markers(new GitProvenance(Tree.randomId(), "origin", "main", "1234", null, null, null))
              .after(a -> {
                  assertThat(a).startsWith("~~>");
                  return a;
              })
          )
        );
    }

    @Test
    void selectInJava() {
        rewriteRun(
          spec -> spec.dataTable(DatabaseColumnsUsed.Row.class, rows -> {
              assertThat(rows).hasSize(1);
              DatabaseColumnsUsed.Row row = rows.get(0);
              assertThat(row.getLineNumber()).isEqualTo(2);
              assertThat(row.getOperation()).isEqualTo(DatabaseColumnsUsed.Operation.SELECT);
              assertThat(row.getTable()).isEqualTo("users");
              assertThat(row.getColumn()).isEqualTo("first_name");
          }).cycles(1).expectedCyclesThatMakeChanges(1),
          java(
            //language=java
            """
              class Test {
                String aSelect = \"""
                   SELECT distinct(first_name)
                   FROM users
                   WHERE user_id = :userId
                   AND :user_type = ANY (user_types)
                   \""";
              }
              """,
            spec -> spec.after(a -> {
                assertThat(a).contains("/*~~>*/");
                return a;
            })
          )
        );
    }

    @Test
    void selectInYaml() {
        rewriteRun(
          spec -> spec.dataTable(DatabaseColumnsUsed.Row.class, rows -> {
              assertThat(rows).hasSize(1);
              DatabaseColumnsUsed.Row row = rows.get(0);
              assertThat(row.getLineNumber()).isEqualTo(2);
              assertThat(row.getOperation()).isEqualTo(DatabaseColumnsUsed.Operation.SELECT);
              assertThat(row.getTable()).isEqualTo("users");
              assertThat(row.getColumn()).isEqualTo("first_name");
          }).cycles(1).expectedCyclesThatMakeChanges(1),
          yaml(
            //language=yaml
            """
              foo: bar
              query: >
                  SELECT distinct(first_name)
                  FROM users
                  WHERE user_id = :userId
                  AND :user_type = ANY (user_types)
              """,
            spec -> spec.after(a -> {
                assertThat(a).contains("~~>");
                return a;
            })
          )
        );
    }

    @Test
    void update() {
        //noinspection SqlDialectInspection
        rewriteRun(
          spec -> spec.dataTable(DatabaseColumnsUsed.Row.class, rows -> {
              assertThat(rows).hasSize(1);
              DatabaseColumnsUsed.Row row = rows.get(0);
              assertThat(row.getOperation()).isEqualTo(DatabaseColumnsUsed.Operation.UPDATE);
              assertThat(row.getTable()).isEqualTo("user_update_jobs");
              assertThat(row.getColumn()).isEqualTo("state");
          }),
          text(
            // language=sql
            """
              UPDATE user_update_jobs
              SET state = 'CANCELED'
              WHERE state IN ('QUEUED', 'ORPHANED', 'PROCESSING')
              AND job_id = :jobId
              """,
            spec -> spec
              .path("update.sql")
              .after(a -> {
                  assertThat(a).startsWith("~~>");
                  return a;
              })
          )
        );
    }

    @Test
    void delete() {
        //noinspection SqlDialectInspection
        rewriteRun(
          spec -> spec.dataTable(DatabaseColumnsUsed.Row.class, rows -> {
              assertThat(rows).hasSize(1);
              DatabaseColumnsUsed.Row row = rows.get(0);
              assertThat(row.getOperation()).isEqualTo(DatabaseColumnsUsed.Operation.DELETE);
              assertThat(row.getTable()).isEqualTo("users");
              assertThat(row.getColumn()).isNull();
          }),
          text(
            // language=sql
            """
              DELETE FROM users
              WHERE email = :email
              """,
            spec -> spec
              .path("delete.sql")
              .after(a -> {
                  assertThat(a).startsWith("~~>");
                  return a;
              })
          )
        );
    }
}
