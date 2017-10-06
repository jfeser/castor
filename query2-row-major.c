#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <sys/mman.h>
#include <unistd.h>

#include "uthash.h"

typedef struct {
  char app_user_id[11]; // app_user_id
  int app_id; // app_id
  long e3;
  char user_state[10];
  char customer_key[11];
  UT_hash_handle hh;
} app_users_row;

typedef struct {
  char app_user_id[11];
  char e9[16];
  int user_score_trips;
  char e11;
} app_user_scores_row;

typedef app_users_row app_users[100000]; // app_users
typedef app_user_scores_row app_user_scores[1000000]; // app_user_scores
typedef struct {
  app_users app_users;
  app_user_scores app_user_scores;
} t95[1];

/* SELECT */
/* app_users.customer_key AS expr1, */
/*   app_users.registration_date AS expr2, */
/*   max(app_user_device_settings.last_request_date) AS expr3, */
/*   app_user_scores.user_score_trips AS expr4, */
/*   app_users.user_state AS State */
/*   FROM app_users */
/*   LEFT OUTER JOIN */
/*   app_user_scores */
/*   on app_users.app_user_id=app_user_scores.app_user_id */
/*   LEFT OUTER JOIN */
/*   app_user_device_settings */
/*   on app_users.app_user_id=app_user_device_settings.app_user_id */
/*   WHERE */
/*   (app_users.app_id = 34) AND */
/*   (app_user_scores.user_score_interval = :arg1) AND */
/*   (app_user_scores.user_score_honoring_drive_labels) */
/*   GROUP BY 1,2,4,5 */
/*   ORDER BY 3 desc */

int main(int argc, char **argv) {
  if (argc != 2) {
    printf("Usage: builder FILE");
    return 1;
  }

  char *path = argv[1];

  int fd = open(path, O_RDWR);
  if (fd < 0) {
    printf("Failed to open file %s. %s", path, strerror(errno));
    return 1;
  }
  void *m_ptr =
    mmap(NULL, sizeof(t95), PROT_WRITE | PROT_READ, MAP_SHARED, fd, 0);
  if (m_ptr == MAP_FAILED) {
    printf("Mmap failed. %s", strerror(errno));
    return 1;
  }
  t95 *data = (t95 *)m_ptr;

  // Seq Scan on app_users => Filter (app_id = 34) => Hash
  app_users_row *hash_tbl = NULL;
  for (int i = 0; i < 100000; i++) {
    app_users_row *val = &(*data)[0].app_users[i];
    if (val->app_id == 34) {
      HASH_ADD_STR(hash_tbl, app_user_id, val);
    }
  }

  // Seq scan on app_user_scores
  for (int i = 0; i < 1000000; i++) {
    app_user_scores_row *lhs = &(*data)[0].app_user_scores[i];
    app_users_row *rhs = NULL;
    HASH_FIND_STR(hash_tbl, lhs->app_user_id, rhs);
    if (rhs != NULL) {
      printf("%s | -- | %d | %s\n", rhs->customer_key, lhs->user_score_trips, rhs->user_state);
    }
  }

  return 0;
}
