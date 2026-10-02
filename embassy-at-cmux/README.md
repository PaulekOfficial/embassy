# `embassy-at-cmux`


## Interoperability

This crate can run on any executor.

It supports any serial port implementing [`embedded-io-async`](https://crates.io/crates/embedded-io-async).

## Buffer sizing

Each channel's RX buffer (`BUF`) must hold a received frame contiguously. The
runner waits for space only when a frame payload is at most `BUF / 2 - 2` bytes.
It drops larger frames that do not fit at once and logs an error. Pick
`BUF >= 2 * (N1 + 2)`, where `N1` is the maximum frame size configured with
`AT+CMUX`. For example, `N1 = 127` needs `BUF >= 258`.
