import { Component } from '@angular/core';
import { Router } from '@angular/router';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { AuthService } from './services/auth.service';

@Component({
  selector: 'app-login',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './login.html',
  styleUrls: ['./login.css']
})
export class LoginComponent {
  username = '';
  password = '';
  loginError = false;

  constructor(private auth: AuthService, private router: Router) {}

  login() {
    const token = btoa(`${this.username}:${this.password}`);

    fetch('/api/auth/check', {
      headers: { Authorization: `Basic ${token}` }
    })
      .then(async res => {
        if (!res.ok) throw new Error();
        const data = await res.json();
        this.loginError = false;
        this.auth.setLoggedIn(this.username, token, data.user.role);
        this.router.navigate(['/app']);
      })
      .catch(() => {
        this.loginError = true;
      });
  }
}