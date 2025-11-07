export interface GeoImage {
  image_id: string;
  title: string;
  latitude: number;
  longitude: number;
  tags: string;
  distance_km?: number;
}

export interface QueryResponse {
  count: number;
  results: GeoImage[];
  query_time_ms: number;
}
