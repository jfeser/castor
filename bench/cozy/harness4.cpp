#include <chrono>
#include <iostream>
#include <vector>
#include <pqxx/pqxx>
#include "stdlib.cpp"
#include "4.cpp"

using namespace std;

typedef query4::_Type1418949 orders_t;
typedef query4::_Type1418950 param_t;

int main(int argc, char **argv) {
  char* db = argv[1];

  vector<orders_t> orders_input;

  try {
    pqxx::connection conn(db);
    pqxx::work txn(conn);

    // Build the internal structure.
    cout << "Loading data..." << flush;
    auto rows =
      txn.exec("select o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment from orders, lineitem where o_orderkey = l_orderkey and l_commitdate < l_receiptdate");
    for (const auto &r : rows) {
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

    txn.commit();
  } catch (const exception &e) {
    cerr << e.what() << endl;
    return 1;
  }

  // Build the query structure.
  cout << "Building query structure..." << flush;
  query4 q(orders_input);
  cout << "done." << endl;

  // Run the query.
  auto start = chrono::high_resolution_clock::now();
  for (int i = 0; i < 1; i++) {
    q.q7(parse_date("1993-07-01"), [](param_t x) { sink(x); });
  }
  auto end = chrono::high_resolution_clock::now();
  chrono::duration<double> diff = end - start;
  cout << "Runtime: " << diff.count() / 1.0 << " s" << endl;

  return 0;
}
