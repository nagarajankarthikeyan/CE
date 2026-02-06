import { Injectable } from '@angular/core';

@Injectable({ providedIn: 'root' })
export class AuthService {
  private readonly LOGIN_KEY = 'isLoggedIn';
  private readonly USERNAME_KEY = 'username';
  private readonly TOKEN_KEY = 'authToken';

  setLoggedIn(username: string, token: string) {
    localStorage.setItem(this.LOGIN_KEY, '1');
    localStorage.setItem(this.USERNAME_KEY, username);
    localStorage.setItem(this.TOKEN_KEY, token);
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

  clear() {
    localStorage.removeItem(this.LOGIN_KEY);
    localStorage.removeItem(this.USERNAME_KEY);
    localStorage.removeItem(this.TOKEN_KEY);
  }
}