import { Provider, AuthResponse } from '@/auth/types/auth.types'

class AuthService {
  private static instance: AuthService
  private readonly baseUrl = '/api/auth'

  private constructor() {}

  public static getInstance(): AuthService {
    if (!AuthService.instance) {
      AuthService.instance = new AuthService()
    }
    return AuthService.instance
  }

  public async connectProvider(provider: Provider): Promise<AuthResponse> {
    try {
      const response = await fetch(`${this.baseUrl}/${provider}/connect`, {
        method: 'GET',
        credentials: 'include',
      })
      const data = await response.json()
      
      if (!response.ok) {
        return {
          status: 'ERROR',
          error: data.error || 'Failed to connect provider',
        }
      }

      return {
        status: 'OK',
        auth_url: data.auth_url,
      }
    } catch (error) {
      return {
        status: 'ERROR',
        error: error instanceof Error ? error.message : 'Unknown error',
      }
    }
  }

  public async signOut(): Promise<void> {
    try {
      await fetch(`${this.baseUrl}/signout`, {
        method: 'POST',
        credentials: 'include',
      })
    } catch (error) {
      console.error('Error signing out:', error)
      throw error
    }
  }
}

export const authService = AuthService.getInstance() 