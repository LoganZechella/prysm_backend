export interface Location {
  lat: number;
  lng: number;
}

export interface PriceInfo {
  min_price?: number;
  max_price?: number;
  currency?: string;
}

export interface Venue {
  name: string;
  address: string;
}

export interface Organizer {
  name: string;
  description?: string;
}

export interface Event {
  id: number;
  title: string;
  description: string;
  start_time: string;
  end_time: string | null;
  location: Location;
  categories: string[];
  price_info: PriceInfo | null;
  source: string;
  source_id: string;
  url: string | null;
  image_url: string | null;
  venue: Venue | null;
  organizer: Organizer | null;
  tags: string[] | null;
  view_count: number;
  like_count: number;
  created_at: string;
  updated_at: string;
} 