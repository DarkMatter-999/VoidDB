use std::io::{self, Write};

pub struct InputBuffer {
    pub buffer: String,
}

impl InputBuffer {
    pub fn new() -> InputBuffer {
        InputBuffer {
            buffer: String::new(),
        }
    }

    pub fn read_input(&mut self) {
        self.buffer.clear();

        print!("db > ");
        io::stdout().flush().unwrap();

        io::stdin()
            .read_line(&mut self.buffer)
            .expect("Error reading input");

        self.buffer = self.buffer.trim().to_string();
    }

    pub fn close(&mut self) {
        self.buffer.clear();
    }
}
