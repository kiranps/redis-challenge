open Unix;
open Printf;
open Thread;
open Tablecloth;

type resp_type =
  | SimpleString(string)
  | Errors(string)
  | BulkString(string)
  | Interger(int)
  | Array(list(resp_type))
  | InvalidType;

let store = Hashtbl.create(999999);

let parse_bulk_string = cin => cin |> input_line |> String.trim;

let rec parse_array = (cin, len) => {
  len === 1
    ? [resp_parser(cin)]
    : [resp_parser(cin), ...parse_array(cin, len - 1)];
}
and resp_parser = cin => {
  let line = cin |> input_line |> String.trim |> String.uncons;
  switch (line) {
  | Some(('*', len)) =>
    Array(parse_array(cin, int_of_string(len)) |> List.reverse)
  | Some(('$', len)) => BulkString(parse_bulk_string(cin))
  | Some((':', number)) => Interger(number |> int_of_string)
  | Some((_, _)) => InvalidType
  | None => InvalidType
  };
};

let sleep_wait = (key, expiry) => {
  expiry |> float_of_int |> string_of_float |> print_endline;
  sleepf(float_of_int(expiry) /. 1000.);
  Hashtbl.replace(store, key, "nil");
};

let execute_command = command => {
  switch (command) {
  | Array([command, ...rest]) =>
    switch (command) {
    | BulkString("ping") => "+PONG\r\n"
    | BulkString("echo") =>
      switch (rest) {
      | [BulkString(x)] => "+" ++ x ++ "\r\n"
      | _ => "+Invalid\r\n"
      }
    | BulkString("set") =>
      print_endline("hello man");
      switch (rest) {
      | [BulkString(key), BulkString(value)] =>
        Hashtbl.add(store, key, value);
        "+OK\r\n";
      | [
          BulkString(key),
          BulkString(value),
          BulkString("px"),
          BulkString(exp),
        ] =>
        let expiry_time = exp |> int_of_string;
        print_endline("expiry");
        Hashtbl.add(store, key, value);
        let _ = Thread.create(sleep_wait(key), expiry_time);
        "+OK\r\n";
      | _ => "+Invalid\r\n"
      };
    | BulkString("get") =>
      switch (rest) {
      | [BulkString(key)] =>
        let value = Hashtbl.find(store, key);
        value == "nil" ? "$-1\r\n" : "+" ++ value ++ "\r\n";
      | _ => "+Invalid\r\n"
      }
    | _ => "+Invalid\r\n"
    }
  | _ => "+Invalid\r\n"
  };
};

let rec handle_commands = s => {
  let cin = in_channel_of_descr(s);
  let cout = out_channel_of_descr(s);
  let redis_command = resp_parser(cin);
  let result = execute_command(redis_command);
  output_string(cout, result);
  flush(cout);
  handle_commands(s);
};

let rec create_connection = sock => {
  let (s, _) = accept(sock);
  printf("Accepted a connection.\n%!");
  let _ = Thread.create(handle_commands, s);
  create_connection(sock);
};

let create_tcp_server = port => {
  Sys.set_signal(Sys.sigpipe, Sys.Signal_ignore);
  let sock = socket(PF_INET, SOCK_STREAM, 0);
  setsockopt(sock, SO_REUSEADDR, true);
  bind(sock, ADDR_INET(inet_addr_of_string("0.0.0.0"), port));
  listen(sock, 5);
  create_connection(sock);
};

let run = () => {
  create_tcp_server(6379);
};
