use VoidDB::input::InputBuffer;
use VoidDB::compiler::*;

fn main() {
    let mut input_buffer = InputBuffer::new();

    let mut table = Table::new();

    loop {
        input_buffer.read_input();

        if input_buffer.buffer.starts_with('.') {
            match do_meta_command(&mut input_buffer) {
                MetaCommandResult::Success => continue,
                MetaCommandResult::UnrecognizedCommand => {
                    println!("Unrecognized command '{}'", input_buffer.buffer);
                    continue;
                }
            }
        }

        match prepare_statement(&input_buffer) {
            Ok(statement) => {
                execute_statement(&statement, &mut table);
                println!("Executed.");
            }
            Err(PrepareResult::UnrecognizedStatement) => {
                println!("Unrecognized keyword at start of '{}'.", input_buffer.buffer);
            }
            _ => {}
        }
    }
}
