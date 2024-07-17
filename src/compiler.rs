use crate::input::InputBuffer;

pub enum MetaCommandResult {
    Success,
    UnrecognizedCommand,
}

pub enum PrepareResult {
    Success,
    UnrecognizedStatement,
    SyntaxError,
}

pub enum ExecuteResult {
    Success,
    TableFull,
}

pub enum StatementType {
    Insert,
    Select,
}

const COLUMN_USERNAME_SIZE: usize = 32;
const COLUMN_EMAIL_SIZE: usize = 255;

pub struct Row {
    pub id: u32,
    pub username: [u8; COLUMN_USERNAME_SIZE],
    pub email: [u8; COLUMN_EMAIL_SIZE],
}

impl Row {
    fn new(id: u32, username: &str, email: &str) -> Self {
        let usernamelen = if username.len() > COLUMN_USERNAME_SIZE { COLUMN_USERNAME_SIZE } else { username.len() };
        let mut username_array = [0u8; COLUMN_USERNAME_SIZE];
        for (i, byte) in username[0..usernamelen].as_bytes().iter().enumerate() {
            username_array[i] = *byte;
        }

        let emaillen = if email.len() > COLUMN_EMAIL_SIZE { COLUMN_EMAIL_SIZE } else { email.len() };
        let mut email_array = [0u8; COLUMN_EMAIL_SIZE];
        for (i, byte) in email[0..emaillen].as_bytes().iter().enumerate() {
            email_array[i] = *byte;
        }

        Row {
            id,
            username: username_array,
            email: email_array,
        }
    }

    fn serialize(&self) -> Vec<u8> {
        let mut result = vec![];
        result.extend_from_slice(&self.id.to_le_bytes());
        result.extend_from_slice(&self.username);
        result.extend_from_slice(&self.email);
        result
    }

    fn deserialize(data: &[u8]) -> Self {
        let id = u32::from_le_bytes(data[0..4].try_into().unwrap());
        let mut username = [0u8; COLUMN_USERNAME_SIZE];
        username.copy_from_slice(&data[4..36]);
        let mut email = [0u8; COLUMN_EMAIL_SIZE];
        email.copy_from_slice(&data[36..291]);

        Row { id, username, email }
    }

    fn print(&self) {
        let username_str = String::from_utf8_lossy(&self.username);
        let email_str = String::from_utf8_lossy(&self.email);
        println!("({}, {}, {})", self.id, username_str, email_str);
    }
}

const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROW_SIZE: usize = 291;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

pub struct Table {
    num_rows: usize,
    pages: [Option<Vec<u8>>; TABLE_MAX_PAGES],
}

impl Table {
    pub fn new() -> Self {
        Table {
            num_rows: 0,
            pages: {
                const NONE: Option<Vec<u8>> = None;
                let mut pages = [NONE; TABLE_MAX_PAGES];
                for page in &mut pages {
                    *page = None;
                }
                pages
            },
        }
    }

    fn row_slot(&mut self, row_num: usize) -> &mut [u8] {
        let page_num = row_num / ROWS_PER_PAGE;
        let page_offset = row_num % ROWS_PER_PAGE * ROW_SIZE;

        if self.pages[page_num].is_none() {
            self.pages[page_num] = Some(vec![0; PAGE_SIZE]);
        }

        self.pages[page_num].as_mut().unwrap().get_mut(page_offset..page_offset + ROW_SIZE).unwrap()
    }
}

pub struct Statement {
    pub typ: StatementType,
    pub row_to_insert: Option<Row>,
}

pub fn do_meta_command(input_buffer: &mut InputBuffer) -> MetaCommandResult {
    if input_buffer.buffer == ".exit" {
        input_buffer.close();
        std::process::exit(0);
    } else {
        MetaCommandResult::UnrecognizedCommand
    }
}

pub fn prepare_statement(input_buffer: &InputBuffer) -> Result<Statement, PrepareResult> {
    if input_buffer.buffer.starts_with("insert") {
        let mut args = input_buffer.buffer.split_whitespace();
        args.next(); // skip insert
        
        let id = match args.next().and_then(|s| s.parse().ok()) {
            Some(id) => id,
            None => return Err(PrepareResult::SyntaxError),
        };
        
        let username = match args.next() {
            Some(username) => username,
            None => return Err(PrepareResult::SyntaxError),
        };
        
        let email = match args.next() {
            Some(email) => email,
            None => return Err(PrepareResult::SyntaxError),
        };

        if args.next().is_some() {
            return Err(PrepareResult::SyntaxError);
        }

        let row = Row::new(id, username, email);
 
        Ok(Statement { typ: StatementType::Insert , row_to_insert: Some(row)})
    } else if input_buffer.buffer == "select" {
        Ok(Statement { typ: StatementType::Select, row_to_insert: None })
    } else {
        Err(PrepareResult::UnrecognizedStatement)
    }
}

fn execute_insert(statement: &Statement, table: &mut Table) -> ExecuteResult {
    if table.num_rows >= TABLE_MAX_ROWS {
        return ExecuteResult::TableFull;
    }

    match &statement.row_to_insert {
        Some(row) => {
            let slot = table.row_slot(table.num_rows);
            slot.copy_from_slice(&row.serialize());
            table.num_rows += 1;
            ExecuteResult::Success
        },
        None => ExecuteResult::TableFull
    }
}

fn execute_select(_statement: &Statement, table: &mut Table) -> ExecuteResult {
    for i in 0..table.num_rows {
        let slot = table.row_slot(i);
        let row = Row::deserialize(slot);
        row.print();
    }
    ExecuteResult::Success
}

pub fn execute_statement(statement: &Statement, table: &mut Table) -> ExecuteResult {
    match statement.typ {
        StatementType::Insert => execute_insert(statement, table),
        StatementType::Select => execute_select(statement, table),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prepare_select() {
        let mut input_buffer = InputBuffer::new();
        input_buffer.buffer = "select".to_string();
        
        let exec_status = prepare_statement(&input_buffer);

        assert!(matches!(exec_status, Ok(Statement { typ: StatementType::Select, row_to_insert: None })));
    }

    #[test]
    fn test_prepare_insert() {
        let mut input_buffer = InputBuffer::new();
        input_buffer.buffer = "insert 1 username email@email.com".to_string();
        
        let exec_status = prepare_statement(&input_buffer);

        assert!(matches!(exec_status, Ok(Statement { typ: StatementType::Insert, row_to_insert: Some(_) })));
    }


    #[test]
    fn test_insert() {
        let mut table = Table::new();
        let row = Row::new(1, "username", "email@email.com");
 
        let statement = Statement { typ: StatementType::Insert , row_to_insert: Some(row)};
        let exec_status = execute_statement(&statement, &mut table);

        assert!(matches!(exec_status, ExecuteResult::Success));
    } 
}
