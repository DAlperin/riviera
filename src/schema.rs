use std::collections::HashMap;

use crate::expr::DataType;

#[derive(Debug, Clone, PartialEq)]
pub struct Schema {
    pub fields: Vec<Field>,
}

impl Schema {
    pub fn new(fields: Vec<Field>) -> Self {
        Self { fields }
    }

    pub fn fields_by_name_and_qualifier(&self, qualifier: &str, name: &str) -> Vec<Field> {
        self.fields
            .iter()
            .filter(|f| f.qualifier == Some(qualifier.to_string()) && f.name == name)
            .cloned()
            .collect()
    }

    pub fn fields_by_name(&self, name: &str) -> Vec<Field> {
        self.fields
            .iter()
            .filter(|f| f.name == name)
            .cloned()
            .collect()
    }

    pub fn merge(&mut self, other: &Self) -> Self {
        for field in &other.fields {
            if self.fields.contains(field) {
                continue;
            }
            self.fields.push(field.clone());
        }
        self.clone()
    }

    pub fn requalify(&self, qualifier: &str) -> Self {
        let new_fields = self
            .fields
            .iter()
            .map(|f| Field::new_with_qualifier(Some(qualifier), &f.name, f.data_type, f.nullable))
            .collect::<Vec<_>>();
        Self { fields: new_fields }
    }

    pub fn display(&self) -> String {
        let fields = self
            .fields
            .iter()
            .map(|f| f.name.to_string())
            .collect::<Vec<_>>();
        format!("Schema({})", fields.join(", "))
    }

    pub fn column(&self, index: usize) -> Field {
        self.fields[index].clone()
    }

    pub fn to_arrow(&self) -> arrow_schema::Schema {
        let fields = self
            .fields
            .iter()
            .map(|f| {
                let arrow_type = match f.data_type {
                    DataType::Int32 => arrow_schema::DataType::Int32,
                    DataType::Utf8 => arrow_schema::DataType::Utf8,
                };
                arrow_schema::Field::new(&f.name, arrow_type, f.nullable)
            })
            .collect::<Vec<_>>();
        arrow_schema::Schema::new(fields)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    pub name: String,
    pub qualifier: Option<String>,
    pub data_type: DataType,
    pub nullable: bool,
}

impl Field {
    pub fn new(name: &str, data_type: DataType, nullable: bool) -> Self {
        Self {
            name: name.to_string(),
            qualifier: None,
            data_type,
            nullable,
        }
    }

    pub fn new_with_qualifier(
        qualifier: Option<&str>,
        name: &str,
        data_type: DataType,
        nullable: bool,
    ) -> Self {
        Self {
            name: name.to_string(),
            qualifier: qualifier.map(|q| q.to_string()),
            data_type,
            nullable,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableProvider {
    // TODO: There should be a type for tables instead of going directly to Schema
    pub tables: HashMap<String, Schema>,
}

impl TableProvider {
    pub fn new(tables: HashMap<String, Schema>) -> Self {
        let schemas_with_qual = tables
            .iter()
            .map(|(name, schema)| {
                let new_schema = schema
                    .fields
                    .iter()
                    .map(|f| match &f.qualifier {
                        //TODO: there could be multiple levels of qualifiers
                        Some(..) => f.clone(),
                        None => {
                            Field::new_with_qualifier(Some(name), &f.name, f.data_type, f.nullable)
                        }
                    })
                    .collect::<Vec<_>>();
                (name.clone(), Schema { fields: new_schema })
            })
            .collect::<HashMap<_, _>>();

        Self {
            tables: schemas_with_qual,
        }
    }
}
