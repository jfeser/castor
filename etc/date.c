#include "date.h"

void civil_from_days(int z, int* year, int* mon, int* day)
{
  z += 719468;
  const int era = (z >= 0 ? z : z - 146096) / 146097;
  const unsigned doe = (z - era * 146097);          // [0, 146096]
  const unsigned yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;  // [0, 399]
  const int y = (yoe) + era * 400;
  const unsigned doy = doe - (365*yoe + yoe/4 - yoe/100);                // [0, 365]
  const unsigned mp = (5*doy + 2)/153;                                   // [0, 11]
  const unsigned d = doy - (153*mp+2)/5 + 1;                             // [1, 31]
  const unsigned m = mp + (mp < 10 ? 3 : -9);                            // [1, 12]
  *year = y + (m <= 2);
  *mon = m;
  *day = d;
}

int days_from_civil(int y, unsigned m, unsigned d)
{
  y -= m <= 2;
  const int era = (y >= 0 ? y : y-399) / 400;
  const unsigned yoe = (y - era * 400);      // [0, 399]
  const unsigned doy = (153*(m + (m > 2 ? -3 : 9)) + 2)/5 + d-1;  // [0, 365]
  const unsigned doe = yoe * 365 + yoe/4 - yoe/100 + doy;         // [0, 146096]
  return era * 146097 + doe - 719468;
}
