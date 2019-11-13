#include <chrono>
#include <iostream>
#include <vector>
#include <pqxx/pqxx>
#include "stdlib.cpp"
#include "4.cpp"

using namespace std;

typedef query4::_Type1418951 lineitem_t;
typedef query4::_Type1418949 orders_t;
typedef query4::_Type1418950 param_t;

int main(int argc, char **argv) {
  char* db = argv[1];

  vector<orders_t> orders_input;
  vector<lineitem_t> lineitem_input;

  try {
    pqxx::connection conn(db);
    pqxx::work txn(conn);

    // Build the part bag.
    cout << "Loading orders..." << flush;
    for (const auto &r : txn.exec("select * from orders")) {
      auto tuple =
        orders_t(r[0].as<int>(),
                 r[1].as<int>(),
                 r[2].as<string>(),
                 r[3].as<float>(),
                 parse_date(r[4].as<string>()),
                 r[5].as<string>(),
                 r[6].as<string>(),
                 r[7].as<int>(),
                 r[8].as<string>());
      orders_input.push_back(tuple);
    }
    cout << " done." << endl;

    // Build the lineitem bag.
    cout << "Loading lineitem..." << flush;
    for (const auto &r : txn.exec("select * from lineitem")) {
      auto tuple =
        lineitem_t(r[0].as<int>(),
                   r[1].as<int>(),
                   r[2].as<int>(),
                   r[3].as<int>(),
                   r[4].as<int>(),
                   r[5].as<float>(),
                   r[6].as<float>(),
                   r[7].as<float>(),
                   r[8].as<string>(),
                   r[9].as<string>(),
                   parse_date(r[10].as<string>()),
                   parse_date(r[11].as<string>()),
                   parse_date(r[12].as<string>()),
                   r[13].as<string>(),
                   r[14].as<string>(),
                   r[15].as<string>());
      lineitem_input.push_back(tuple);
    }
    cout << " done." << endl;

    txn.commit();
  } catch (const exception &e) {
    cerr << e.what() << endl;
    return 1;
  }

  // Build the query structure.
  cout << "Building query structure..." << flush;
  query4 q(lineitem_input, orders_input);
  cout << "done." << endl;

  // Run the query.
  auto start = chrono::high_resolution_clock::now();
  for (int i = 0; i < 100; i++) {
    q.q7(parse_date("1993-07-01"), [](param_t x) { sink(x); });
  }
  auto end = chrono::high_resolution_clock::now();
  chrono::duration<double> diff = end - start;
  cout << "Runtime: " << diff.count() / 100.0 << " s" << endl;

  return 0;
}
