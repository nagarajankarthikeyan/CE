import { Routes } from '@angular/router';
import { LoginComponent } from './login';
import { ChatComponent } from './chat';
import { AdminComponent } from './admin';
import { authGuard } from './services/auth.guard';
import { adminGuard } from './services/adminguard';

export const routes: Routes = [
  { path: 'login', component: LoginComponent },
  { path: 'app', component: ChatComponent, canActivate: [authGuard] },
  { path: 'admin', component: AdminComponent, canActivate: [authGuard, adminGuard] },
  { path: '', redirectTo: 'login', pathMatch: 'full' },
  { path: '**', redirectTo: 'login' }
];
