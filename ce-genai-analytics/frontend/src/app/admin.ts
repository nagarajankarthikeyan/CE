import { Component, OnInit, ChangeDetectorRef } from '@angular/core';
import { Router } from '@angular/router';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { of } from 'rxjs';
import { catchError, finalize, timeout } from 'rxjs/operators';
import { AuthService } from './services/auth.service';

@Component({
  selector: 'app-admin',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './admin.html',
  styleUrls: ['./admin.css']
})
export class AdminComponent implements OnInit {
  users: any[] = [];
  showAddForm = false;
  addPasswordVisible = false;
  loadError = '';
  loading = false;
  creating = false;
  updatingId: number | null = null;
  deletingId: number | null = null;
  showProfileMenu = false;
  username = '';

  newUser = {
    email: '',
    username: '',
    password: '',
    role: 'user'
  };

  editingId: number | null = null;
  editUser = {
    email: '',
    username: '',
    password: '',
    role: 'user',
    isActive: true
  };
  editPasswordVisible = false;

  constructor(
    private http: HttpClient,
    private auth: AuthService,
    private cd: ChangeDetectorRef,
    private router: Router
  ) {}

  ngOnInit() {
    this.username = this.auth.getUsername();
    this.loadUsers();
  }

  loadUsers() {
    this.loading = true;
    this.loadError = '';
    this.http.get<any[]>('/api/admin/users', this.getAuthHeaders())
      .pipe(
        timeout(8000),
        catchError(err => {
          this.loadError = `Failed to load users (${err?.status ?? 'unknown'})`;
          console.error('[Admin] loadUsers failed', err);
          return of([]);
        }),
        finalize(() => {
          this.loading = false;
          this.cd.detectChanges();
        })
      )
      .subscribe(res => {
        this.users = (res || []).map(u => ({ ...u }));
        this.cd.detectChanges();
      });
  }

  createUser() {
    if (this.creating) return;
    this.creating = true;
    this.http.post('/api/admin/users', this.newUser, this.getAuthHeaders())
      .pipe(
        finalize(() => {
          this.creating = false;
          this.cd.detectChanges();
        })
      )
      .subscribe({
        next: () => {
          this.showAddForm = false;
          this.addPasswordVisible = false;
          this.newUser = { email: '', username: '', password: '', role: 'user' };
          this.loadUsers();
        },
        error: err => {
          console.error('Create user failed', err);
        }
      });
  }

  startEdit(user: any) {
    this.editingId = user.UserID;
    this.editUser = {
      email: user.Email || '',
      username: user.Username || '',
      password: '',
      role: user.Role || 'user',
      isActive: this.isActive(user)
    };
    this.editPasswordVisible = false;
  }

  cancelEdit() {
    this.editingId = null;
    this.editPasswordVisible = false;
    this.editUser = { email: '', username: '', password: '', role: 'user', isActive: true };
  }

  saveEdit(userId: number) {
    if (this.updatingId !== null) return;
    this.updatingId = userId;
    const payload: any = {
      email: this.editUser.email,
      username: this.editUser.username,
      role: this.editUser.role,
      is_active: this.editUser.isActive ? 1 : 0
    };
    if (this.editUser.password && this.editUser.password.trim()) {
      payload.password = this.editUser.password;
    }

    this.http.put(`/api/admin/users/${userId}`, payload, this.getAuthHeaders())
      .pipe(
        finalize(() => {
          this.updatingId = null;
          this.cd.detectChanges();
        })
      )
      .subscribe({
        next: () => {
          this.cancelEdit();
          this.loadUsers();
        },
        error: err => {
          console.error('Update user failed', err);
        }
      });
  }

  toggleActive(user: any) {
    this.http.put(
      `/api/admin/users/${user.UserID}/status?is_active=${!user.IsActive}`,
      {},
      this.getAuthHeaders()
    ).subscribe(() => this.loadUsers());
  }

  deleteUser(id: number) {
    if (this.deletingId !== null) return;
    if (!confirm('Delete this user?')) return;
    this.deletingId = id;
    this.http.delete(`/api/admin/users/${id}`, this.getAuthHeaders())
      .pipe(
        finalize(() => {
          this.deletingId = null;
          this.cd.detectChanges();
        })
      )
      .subscribe({
        next: () => this.loadUsers(),
        error: err => console.error('Delete user failed', err)
      });
  }

  private getAuthHeaders() {
    return {
      headers: new HttpHeaders({
        Authorization: `Basic ${this.auth.getToken()}`
      })
    };
  }

  isActive(user: any): boolean {
    return user?.IsActive === 1 || user?.IsActive === true || user?.IsActive === '1';
  }

  toggleProfileMenu() {
    this.showProfileMenu = !this.showProfileMenu;
  }

  goToChat() {
    this.showProfileMenu = false;
    this.router.navigate(['/app']);
  }

  logout() {
    this.showProfileMenu = false;
    this.auth.clear();
    this.router.navigate(['/login']);
  }

}
