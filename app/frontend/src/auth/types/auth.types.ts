export type Provider = 'spotify' | 'google' | 'linkedin'

export interface AuthResponse {
  auth_url?: string
  error?: string
  status?: 'OK' | 'ERROR'
}

export interface User {
  id: string
  email: string
  providers: Provider[]
}

export interface AuthState {
  user: User | null
  isAuthenticated: boolean
  isLoading: boolean
} 