import axios from 'axios';

// Create axios instance with default config
const api = axios.create({
  baseURL: '/api',  // This will be proxied to http://localhost:8000/api
  headers: {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
  },
  withCredentials: true,
});

// Add request interceptor to add token
api.interceptors.request.use(
  (config) => {
    const token = localStorage.getItem('auth_token');
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
  },
  (error) => {
    return Promise.reject(error);
  }
);

// Add response interceptor to handle token refresh
api.interceptors.response.use(
  (response) => {
    return response;
  },
  async (error) => {
    const originalRequest = error.config;

    // If error is 401 and we haven't tried to refresh token yet
    if (error.response?.status === 401 && !originalRequest._retry) {
      originalRequest._retry = true;

      try {
        // Try to refresh the token
        const response = await api.post('/auth/session/refresh');
        const { access_token } = response.data;

        if (access_token) {
          localStorage.setItem('auth_token', access_token);
          originalRequest.headers.Authorization = `Bearer ${access_token}`;
          return api(originalRequest);
        }
      } catch (refreshError) {
        // If refresh fails, clear token and let the error propagate
        localStorage.removeItem('auth_token');
      }
    }

    return Promise.reject(error);
  }
);

export default api; 