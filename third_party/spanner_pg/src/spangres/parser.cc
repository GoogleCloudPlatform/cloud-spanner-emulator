#include "third_party/spanner_pg/src/spangres/parser.h"

extern "C" {
void add_token_end_start_spangres(int location,
                                  struct SpangresTokenLocations* locations) {
  if (locations == nullptr) {
    return;
  }

  if (locations->last_start_location.has_value()) {
    locations->start_end_pairs[locations->last_start_location.value()] =
        location;
  }
  locations->last_start_location = location;
}
}
