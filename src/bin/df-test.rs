// use arrow_schema::{DataType, Field, Schema};
use datafusion_common::arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{plan_err, DataFusionError, Result};
use datafusion_expr::WindowUDF;
use datafusion_expr::{
    logical_plan::builder::LogicalTableSource, AggregateUDF, ScalarUDF, TableSource,
};
use datafusion_sql::{
    planner::{ContextProvider, SqlToRel},
    sqlparser::{dialect::GenericDialect, parser::Parser},
    TableReference,
};
use std::vec;
use std::{collections::HashMap, sync::Arc};

fn main() {
    // let sql = "SELECT posts.name, likes.postcount FROM posts LEFT JOIN (SELECT post_id, COUNT(*) AS postcount FROM likes GROUP BY post_id) AS likes ON posts.id = likes.post_id";
    // testing implicit joins
    // let sql = "SELECT * FROM posts, likes WHERE posts.id = likes.post_id";

    // let sql = "SELECT post_id, COUNT(*) AS postcount FROM likes GROUP BY post_id;";
    // let sql = "SELECT posts.name, COUNT(likes.id) FROM posts LEFT JOIN likes ON posts.id = likes.post_id GROUP BY posts.name;";

    let sql = "SELECT name, postcount * 2 FROM posts LEFT JOIN (SELECT post_id, COUNT(id) AS postcount FROM likes GROUP BY post_id) AS likes ON posts.id = likes.post_id";

    // parse the SQL
    let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...
    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    let statement = &ast[0];
    println!("{statement:#?}");

    // create a logical query plan
    let context_provider = MyContextProvider::new();
    let sql_to_rel = SqlToRel::new(&context_provider);
    let plan = sql_to_rel.sql_statement_to_plan(statement.clone()).unwrap();
    // plan.
    let graphviz = format!("{}", plan.display_graphviz());
    println!("{}", graphviz);

    // show the plan
    // println!("{plan:?}");
    println!("{}", plan.display_indent_schema());
}

struct MyContextProvider {
    options: ConfigOptions,
    tables: HashMap<String, Arc<dyn TableSource>>,
}

impl MyContextProvider {
    fn new() -> Self {
        let mut tables = HashMap::new();
        tables.insert(
            "likes".to_string(),
            create_table_source(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("post_id", DataType::Int32, false),
            ]),
        );
        tables.insert(
            "posts".to_string(),
            create_table_source(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, false),
            ]),
        );

        Self {
            tables,
            options: Default::default(),
        }
    }
}

fn create_table_source(fields: Vec<Field>) -> Arc<dyn TableSource> {
    Arc::new(LogicalTableSource::new(Arc::new(
        Schema::new_with_metadata(fields, HashMap::new()),
    )))
}

impl ContextProvider for MyContextProvider {
    fn get_table_provider(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        match self.tables.get(name.table()) {
            Some(table) => Ok(table.clone()),
            _ => plan_err!("Table not found: {}", name.table()),
        }
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
        None
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }

    fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &self.options
    }
}
