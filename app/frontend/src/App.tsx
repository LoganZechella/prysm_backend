import { ChakraProvider } from '@chakra-ui/react'
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom'
import { initSuperTokens } from '@/auth/config/supertokens'
import AuthDashboard from '@/components/AuthDashboard'
import Recommendations from '@/components/Recommendations'
import OAuthCallback from '@/components/OAuthCallback'
import { useAuth } from '@/auth/hooks/useAuth'

// Initialize SuperTokens
initSuperTokens()

const PrivateRoute = ({ children }: { children: React.ReactNode }) => {
  const { isAuthenticated } = useAuth()
  return isAuthenticated ? <>{children}</> : <Navigate to="/auth/dashboard" />
}

const App = () => {
  return (
    <ChakraProvider>
      <Router>
        <Routes>
          <Route path="/auth/dashboard" element={<AuthDashboard />} />
          <Route path="/auth/callback" element={<OAuthCallback />} />
          <Route
            path="/recommendations"
            element={
              <PrivateRoute>
                <Recommendations />
              </PrivateRoute>
            }
          />
          <Route path="/" element={<Navigate to="/auth/dashboard" />} />
          <Route path="*" element={<Navigate to="/auth/dashboard" />} />
        </Routes>
      </Router>
    </ChakraProvider>
  )
}

export default App
