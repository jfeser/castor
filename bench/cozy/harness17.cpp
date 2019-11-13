#include <chrono>
#include <iostream>
#include <vector>
#include <pqxx/pqxx>
#include "stdlib.cpp"
#include "17.cpp"

using namespace std;

typedef query17::_Type89930 part_t;
typedef query17::_Type89933 lineitem_t;

int main(int argc, char **argv) {
  char* db = argv[1];
  vector<part_t> part_input;
  vector<lineitem_t> lineitem_input;
  std::unordered_map< int , float , std::hash<int > > map1;
  std::unordered_map< int , float , std::hash<int > > map2;

  try {
    pqxx::connection conn(db);
    pqxx::work txn(conn);

    // Build the part bag.
    cout << "Loading part..." << flush;
    for (const auto &r : txn.exec("select * from part")) {
      auto tuple =
        part_t(r[0].as<int>(),
               r[1].as<string>(),
               r[2].as<string>(),
               r[3].as<string>(),
               r[4].as<string>(),
               r[5].as<int>(),
               r[6].as<string>(),
               r[7].as<float>(),
               r[8].as<string>());
      part_input.push_back(tuple);
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

    // Build map1
    cout << "Loading map1..." << flush;
    for (const auto &r : txn.exec("select l_partkey, count(*) from lineitem group by l_partkey")) {
      map1[r[0].as<int>()] = r[1].as<float>();
    }
    cout << " done." << endl;

    // Build map2
    cout << "Loading map2..." << flush;
    for (const auto &r : txn.exec("select l_partkey, sum(l_quantity) from lineitem group by l_partkey")) {
      map2[r[0].as<int>()] = r[1].as<float>();
    }
    cout << " done." << endl;

  } catch (const exception &e) {
    cerr << e.what() << endl;
    return 1;
  }

  // Build the query structure.
  cout << "Building query structure..." << flush;
  query17 q(lineitem_input, part_input);
  cout << "done." << endl;

  // Run the query.
  auto start = chrono::high_resolution_clock::now();
  for (int i = 0; i < 100; i++) {
    sink(q.q6("Brand#23", "MED BOX"));
  }
  auto end = chrono::high_resolution_clock::now();
  chrono::duration<double> diff = end - start;
  cout << "Runtime: " << diff.count() / 100.0 << " s" << endl;

  return 0;
}
