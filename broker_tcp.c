#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/select.h>

#define PUERTO 7000
#define MAX_CLIENTES 50
#define TAM_BUFFER 512
#define MAX_TEMA 64
#define MAX_TEMAS_POR_CLIENTE 10

typedef struct {
    int descriptor_socket; /* -1 = posición libre */
    char temas[MAX_TEMAS_POR_CLIENTE][MAX_TEMA]; /* partidos suscritos */
    int cantidad_temas;
    char entrada[TAM_BUFFER]; /* acumula bytes hasta completar una línea */
    int longitud_entrada;
    char ip[INET_ADDRSTRLEN]; /* se guarda al aceptar (luego ya no se puede consultar) */
    int puerto;
} Cliente;

Cliente lista_clientes[MAX_CLIENTES];

/* Funciones auxiliares */

void limpiar_cliente(int i) {
    lista_clientes[i].descriptor_socket = -1;
    lista_clientes[i].cantidad_temas = 0;
    lista_clientes[i].longitud_entrada = 0;
    lista_clientes[i].ip[0] = '\0';
    lista_clientes[i].puerto = 0;
    for (int t = 0; t < MAX_TEMAS_POR_CLIENTE; t++)
        lista_clientes[i].temas[t][0] = '\0';
}

void inicializar_clientes() {
    for (int i = 0; i < MAX_CLIENTES; i++) limpiar_cliente(i);
}

void desconectar_cliente(int i) {
    printf("[-] Cliente desconectado: Socket %d (%s:%d)\n",
           lista_clientes[i].descriptor_socket, lista_clientes[i].ip, lista_clientes[i].puerto);
    close(lista_clientes[i].descriptor_socket);
    limpiar_cliente(i);
}

void aceptar_nueva_conexion(int socket_servidor) {
    struct sockaddr_in dir;
    socklen_t lg = sizeof(dir);     /* se reinicia en cada accept (es parámetro de entrada y salida) */
    int nuevo = accept(socket_servidor, (struct sockaddr *)&dir, &lg);
    if (nuevo < 0) { perror("Error en accept"); return; }

    for (int i = 0; i < MAX_CLIENTES; i++) {
        if (lista_clientes[i].descriptor_socket == -1) {
            limpiar_cliente(i);
            lista_clientes[i].descriptor_socket = nuevo;
            inet_ntop(AF_INET, &dir.sin_addr, lista_clientes[i].ip, sizeof(lista_clientes[i].ip));
            lista_clientes[i].puerto = ntohs(dir.sin_port);
            printf("[+] Nueva conexión: Socket %d (%s:%d)\n", nuevo,
                   lista_clientes[i].ip, lista_clientes[i].puerto);
            return;
        }
    }
    /* Lista llena: hay que cerrar el socket o se filtra el descriptor */
    printf("[!] Lista de clientes llena, conexión rechazada\n");
    close(nuevo);
}

/* Envía TODO el texto. MSG_NOSIGNAL evita que un socket roto mate al broker (SIGPIPE).
 * Devuelve -1 si falló. */
int enviar_completo(int socket_fd, const char *texto) {
    size_t total = strlen(texto), enviados = 0;
    while (enviados < total) {
        ssize_t n = send(socket_fd, texto + enviados, total - enviados, MSG_NOSIGNAL);
        if (n < 0) 
            return -1;
        enviados += n;
    }
    return 0;
}

/* Procesa UNA línea ya completa (sin el '\n'). */
void procesar_linea(int i, char *linea) {
    int socket_emisor = lista_clientes[i].descriptor_socket;

    if (strncmp(linea, "SUB ", 4) == 0) {
        char tema[MAX_TEMA];
        if (sscanf(linea + 4, "%63s", tema) != 1) {
            printf("[!] Socket %d: SUB sin tema\n", socket_emisor);
            return;
        }
        for (int t = 0; t < lista_clientes[i].cantidad_temas; t++) {
            if (strcmp(lista_clientes[i].temas[t], tema) == 0) {
                printf("[SUB] Socket %d ya estaba suscrito a '%s'\n", socket_emisor, tema);
                return;
            }
        }
        if (lista_clientes[i].cantidad_temas >= MAX_TEMAS_POR_CLIENTE) {
            printf("[!] Socket %d alcanzó el límite de suscripciones (%d)\n",
                   socket_emisor, MAX_TEMAS_POR_CLIENTE);
            return;
        }
        int pos = lista_clientes[i].cantidad_temas++;
        strncpy(lista_clientes[i].temas[pos], tema, MAX_TEMA - 1);
        lista_clientes[i].temas[pos][MAX_TEMA - 1] = '\0';
        printf("[SUB] Socket %d suscrito a '%s' (total temas: %d)\n",
               socket_emisor, tema, lista_clientes[i].cantidad_temas);
    }
    else if (strncmp(linea, "PUB ", 4) == 0) {
        char tema[MAX_TEMA], mensaje[TAM_BUFFER];
        /* Los anchos (%63s, %511[...]) evitan desbordar los arreglos */
        if (sscanf(linea + 4, "%63s %511[^\n]", tema, mensaje) != 2) {
            printf("[!] Socket %d: PUB mal formado (falta tema o mensaje)\n", socket_emisor);
            return;
        }
        printf("[PUB] Evento en '%s': %s\n", tema, mensaje);

        char salida[TAM_BUFFER + MAX_TEMA + 8];
        snprintf(salida, sizeof(salida), "[%s] %s\n", tema, mensaje);

        int reenviados = 0;
        for (int j = 0; j < MAX_CLIENTES; j++) {
            if (lista_clientes[j].descriptor_socket == -1) continue;
            for (int t = 0; t < lista_clientes[j].cantidad_temas; t++) {
                if (strcmp(lista_clientes[j].temas[t], tema) == 0) {
                    if (enviar_completo(lista_clientes[j].descriptor_socket, salida) == 0)
                        reenviados++;
                    else
                        desconectar_cliente(j);   /* el suscriptor ya no está */
                    break;
                }
            }
        }
        printf("Reenviado a %d suscriptor(es)\n", reenviados);
    }
    else {
        printf("[!] Socket %d: comando desconocido: '%s'\n", socket_emisor, linea);
    }
}

/* Lee lo que haya y lo acumula; procesa cada línea completa que encuentre. */
void recibir_datos(int i) {
    Cliente *c = &lista_clientes[i];
    int espacio = TAM_BUFFER - 1 - c->longitud_entrada;
    if (espacio <= 0) {   /* línea enorme sin '\n': se descarta */
        printf("[!] Socket %d: línea demasiado larga, descartada\n", c->descriptor_socket);
        c->longitud_entrada = 0;
        return;
    }
    int n = recv(c->descriptor_socket, c->entrada + c->longitud_entrada, espacio, 0);
    if (n <= 0) { 
        desconectar_cliente(i);
         return; 
    }   /* 0 = cerró, <0 = error */

    c->longitud_entrada += n;
    char *inicio = c->entrada;
    char *fin;
    while ((fin = memchr(inicio, '\n', c->entrada + c->longitud_entrada - inicio)) != NULL) {
        *fin = '\0';
        if (fin > inicio && fin[-1] == '\r') fin[-1] = '\0';   /* por si viene de telnet/nc */
            procesar_linea(i, inicio);
        if (c->descriptor_socket == -1) 
            return;    /* se desconectó mientras procesábamos */
        inicio = fin + 1;
    }
    /* Lo que quedó sin '\n' es un mensaje a medias: se mueve al inicio y se espera el resto */
    int resto = c->entrada + c->longitud_entrada - inicio;
    memmove(c->entrada, inicio, resto);
    c->longitud_entrada = resto;
}

int main() {
    struct sockaddr_in direccion_servidor;
    fd_set conjunto_lectura;

    inicializar_clientes();

    int socket_servidor = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_servidor == -1) {         
        perror("Error al crear socket del servidor");
        exit(EXIT_FAILURE);
    }

    int opcion = 1;
    if (setsockopt(socket_servidor, SOL_SOCKET, SO_REUSEADDR, &opcion, sizeof(opcion)) < 0) {
        perror("Error en setsockopt");
        close(socket_servidor);
        exit(EXIT_FAILURE);
    }

    memset(&direccion_servidor, 0, sizeof(direccion_servidor));
    direccion_servidor.sin_family = AF_INET;
    direccion_servidor.sin_addr.s_addr = INADDR_ANY;
    direccion_servidor.sin_port = htons(PUERTO);

    if (bind(socket_servidor, (struct sockaddr *)&direccion_servidor, sizeof(direccion_servidor)) < 0) {
        perror("Error en bind");
        close(socket_servidor);
        exit(EXIT_FAILURE);
    }
    if (listen(socket_servidor, 10) < 0) {
        perror("Error en listen");
        close(socket_servidor);
        exit(EXIT_FAILURE);
    }
    printf("=== BROKER TCP INICIADO EN EL PUERTO %d ===\n", PUERTO);

    while (1) {
        FD_ZERO(&conjunto_lectura);
        FD_SET(socket_servidor, &conjunto_lectura);
        int max_descriptor = socket_servidor;
        for (int i = 0; i < MAX_CLIENTES; i++) {
            int s = lista_clientes[i].descriptor_socket;
            if (s > 0) FD_SET(s, &conjunto_lectura);
            if (s > max_descriptor) max_descriptor = s;
        }

        if (select(max_descriptor + 1, &conjunto_lectura, NULL, NULL, NULL) < 0) {
            perror("Error en select");
            continue;
        }
        if (FD_ISSET(socket_servidor, &conjunto_lectura))
            aceptar_nueva_conexion(socket_servidor);

        for (int i = 0; i < MAX_CLIENTES; i++) {
            int s = lista_clientes[i].descriptor_socket;
            if (s > 0 && FD_ISSET(s, &conjunto_lectura))
                recibir_datos(i);
        }
    }
    close(socket_servidor);
    return 0;
}
