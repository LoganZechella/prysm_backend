export interface PreferredLocation {
  city: string;
  state: string;
  country: string;
  max_distance_km: number;
}

export interface UserPreferences {
  preferred_categories: string[];
  excluded_categories: string[];
  min_price: number;
  max_price: number;
  preferred_location: PreferredLocation;
  preferred_days: string[];
  preferred_times: string[];
  min_rating: number;
} 