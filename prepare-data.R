library(sf)
library(sfarrow)
library(dplyr)
library(mapview)
library(forcats)
library(glue)
full_dataset <- sfarrow::st_read_parquet("data/GeoLife_Trajectories.parquet")|> 
  st_transform(32650)



minX <- 406993 # west
maxX <- 487551 # east
minY <- 4387642 # south
maxY <- 4463488 # north

coords <- st_coordinates(full_dataset)

in_bejing <- coords[,1] > minX & coords[,1] < maxX & coords[,2] > minY & coords[,2] < maxY


full_dataset <- full_dataset |> 
  mutate(in_bejing = in_bejing)


full_dataset_bejing <- full_dataset |> 
  st_drop_geometry() |> 
  summarise(
    in_bejing = all(in_bejing),
    .by = track_id
  )


full_dataset_bejing$in_bejing |> 
  table()


tracks_AOI <- inner_join(
  full_dataset, 
  select(filter(full_dataset_bejing, in_bejing),track_id),
  by = "track_id"
    )



full_bbox <- st_bbox(tracks_AOI) |> 
  st_as_sfc()

tm_shape(full_bbox) + tm_polygons()

transport_modes <- unique(tracks_AOI$transportation_mode)


tracks_AOI_2 <-tracks_AOI |> 
  mutate(
    mode = ifelse(transportation_mode == "taxi", "car", transportation_mode)
  ) |> 
  select(-transportation_mode, -in_bejing) |> 
  filter(mode %in% c("car", "walk", "train", "bus", "subway", "bike"))

track_id <- unique(tracks_AOI_2$track_id)

set.seed(1291)
set <- sample(c("training", "testing", "validation"), length(track_id), prob = c(.50, .25, .25), replace = TRUE)

table(set)



split_df <- tibble(set, track_id) |> 
  mutate(
    partition = sample(1:4, n(), replace = TRUE),
    .by = set
  )

tracks_AOI_3 <- tracks_AOI_2 |> 
  left_join(split_df, by = "track_id")


tracks_AOI_3 |> 
  filter(set == "training") |> 
  select(-set) |>
  (\(x)split(x, x$partition))() |> 
  imap(\(x, y){
    x |> 
      select(-partition) |> 
      st_write(glue("data/tracks_{y}.gpkg"), "training", append = FALSE)
  })
  

tracks_AOI_3 |> 
  filter(set == "testing")  |> 
  select(-set) |>
  (\(x)split(x, x$partition))() |> 
  imap(\(x, y){
    x |> 
      select(-partition) |> 
      st_write(glue("data/tracks_{y}.gpkg"), "testing", append = FALSE)
  })

tracks_AOI_3 |> 
  filter(set == "validation")  |> 
  select(-set) |>
  select(-mode) |> 
  (\(x)split(x, x$partition))() |> 
  imap(\(x, y){
    x |> 
      select(-partition) |> 
      st_write(glue("data/tracks_{y}.gpkg"), "validation", append = FALSE)
  })


tracks_AOI_3 |> 
  filter(set == "validation") |> 
  select(-set) |>
  # select(-mode) |> 
  (\(x)split(x, x$partition))() |> 
  imap(\(x, y){
    x |> 
      select(-partition) |> 
      st_write(glue("data/validation_{y}.gpkg"), "validation", append = FALSE)
  })

