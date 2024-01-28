use std::collections::HashMap;

use crate::{
    expr::DataType,
    logical_plan::planner,
    schema::{Field, Schema, TableProvider},
};
use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};

pub mod expr;
pub mod logical_plan;
pub mod schema;
pub mod stream;
pub mod tree;

fn main() {
    let sql = "SELECT name, postcount FROM posts LEFT JOIN (SELECT post_id, COUNT(id) AS postcount FROM likes GROUP BY post_id) AS likes ON posts.id = likes.post_id";
    // let sql = "SELECT name FROM posts LEFT JOIN (SELECT post_id, COUNT(*) AS postcount FROM likes GROUP BY post_id) AS likes ON posts.id = likes.post_id ";
    // let sql = "SELECT post_id, COUNT(*) AS postcount FROM likes GROUP BY post_id;";
    // let sql = "SELECT name FROM posts;";
    let ast = Parser::parse_sql(&PostgreSqlDialect {}, &sql).unwrap();
    let statement = ast[0].clone();
    println!("{:#?}", statement);

    let provider = TableProvider::new(HashMap::from([
        (
            "posts".to_string(),
            Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, false),
            ]),
        ),
        (
            "likes".to_string(),
            Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("post_id", DataType::Int32, false),
            ]),
        ),
    ]));

    let planner = planner::RivieraPlanner::new(provider);

    let plan = planner.plan_statement(statement).unwrap();
    println!("{plan}");
}
