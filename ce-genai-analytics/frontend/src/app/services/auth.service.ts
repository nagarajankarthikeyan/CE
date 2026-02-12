import { Injectable } from '@angular/core';

@Injectable({ providedIn: 'root' })
export class AuthService {
  private readonly LOGIN_KEY = 'isLoggedIn';
  private readonly USERNAME_KEY = 'username';
  private readonly TOKEN_KEY = 'authToken';
  private readonly ROLE_KEY = 'role';

  setLoggedIn(username: string, token: string, role: string) {
    localStorage.setItem(this.LOGIN_KEY, '1');
    localStorage.setItem(this.USERNAME_KEY, username);
    localStorage.setItem(this.TOKEN_KEY, token);
    localStorage.setItem(this.ROLE_KEY, role);
  }

  isLoggedIn(): boolean {
    return localStorage.getItem(this.LOGIN_KEY) === '1';
  }

  getUsername(): string {
    return localStorage.getItem(this.USERNAME_KEY) || '';
  }

  getToken(): string {
    return localStorage.getItem(this.TOKEN_KEY) || '';
  }

  getRole(): string {
    return localStorage.getItem(this.ROLE_KEY) || 'user';
  }

  clear() {
    localStorage.removeItem(this.LOGIN_KEY);
    localStorage.removeItem(this.USERNAME_KEY);
    localStorage.removeItem(this.TOKEN_KEY);
    localStorage.removeItem(this.ROLE_KEY);
  }
}