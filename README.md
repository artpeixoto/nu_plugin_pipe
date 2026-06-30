# Description
Its a simple nu plugin allows you to create and manage persistent named pipes. 
These pipes won't die at the end of the command like the vanilla nu pipe operator, which means you can use them as outputs or stuff like that for more sophisticated logic. 
They will only die when you

# Todo
- Add peeking
- Add view_count
- Add try-read and try-write
- Fix the issue with imprecise reading
- Add resizing
- Add locking
- Add bytestream functionality
- Add put-back
- fix the locking issues
- Add timeout

# Alternatives
- (cross-stream)[https://crates.io/crates/cross-stream] : much better, full of functionalities. Will probably serve you better.
- thats it i dont know any other
