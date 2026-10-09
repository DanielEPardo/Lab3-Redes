/*
 * broker_udp.c  -  Laboratorio 3 (ISIS-2311L, Uniandes 2026-20)  -  ESQUELETO
 * Grupo: 6
 *
 * Rol (guia oficial, sec. 4.2)
 *   Recibe datagramas de publicadores y suscriptores por UN solo socket UDP y
 *   reenvia cada PUB, sin modificarlo, a las direcciones suscritas al partido.
 *
 * Protocolo (un datagrama = un mensaje completo; no hay que encuadrar)
 *   suscriptor -> broker : "SUB <partido>\n"
 *   publicador -> broker : "PUB <partido> <seq> <texto>\n"
 *   broker -> suscriptor : el mismo datagrama PUB, tal cual llego
 *
 * Diferencia clave con TCP (p.222): no hay listen, accept ni un socket por
 * cliente. El broker distingue a cada suscriptor por la direccion (IP:puerto)
 * que le devuelve recvfrom, y esa direccion es lo que guarda en la tabla.
 *
 *
 * Compilar:  make udp          Ejecutar:  ./broker_udp 5001
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#define MAX_SUSCRIPCIONES  1024
#define DATAGRAMA_MAX      512
#define PARTIDO_MAX        32

/* Una fila de la tabla: "la direccion dir sigue el partido X". */
typedef struct {
    struct sockaddr_in dir;          /* IP y puerto del suscriptor, tal como los dio recvfrom */
    char               partido[PARTIDO_MAX];
} SuscripcionUDP;

SuscripcionUDP suscripciones[MAX_SUSCRIPCIONES];
int            n_suscripciones = 0;
int            sock_broker = -1;     /* el unico socket del broker */

static volatile sig_atomic_t seguir = 1;
static void al_recibir_senal(int s) { (void)s; seguir = 0; }

void imprimir_suscriptores(void);
void procesar_datagrama(char *dgm, size_t lg, const struct sockaddr_in *origen);
int  misma_direccion(const struct sockaddr_in *a, const struct sockaddr_in *b);
void agregar_suscriptor(const struct sockaddr_in *dir, const char *partido);
void reenviar_a_suscriptores(const char *partido, const char *dgm, size_t lg);

static int leer_puerto(const char *txt)
{
    char *fin;
    long p = strtol(txt, &fin, 10);
    if (*fin != '\0' || p < 1 || p > 65535) {
        fprintf(stderr, "Puerto invalido: %s\n", txt);
        exit(1);
    }
    return (int)p;
}

void imprimir_suscriptores(void)
{
    printf("[broker UDP] tabla de suscripciones (%d filas):\n", n_suscripciones);
    for (int i = 0; i < n_suscripciones; i++) {
        char ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &suscripciones[i].dir.sin_addr, ip, sizeof ip);
        printf("           %s:%-5u -> %s\n", ip, ntohs(suscripciones[i].dir.sin_port),
               suscripciones[i].partido);
    }
}

/* Decide que hacer con un datagrama recibido (ya terminado en '\0'). */
void procesar_datagrama(char *dgm, size_t lg, const struct sockaddr_in *origen)
{
    char partido[PARTIDO_MAX];

    if (sscanf(dgm, "SUB %31s", partido) == 1) {
        agregar_suscriptor(origen, partido); // si el datagrama es de suscripcion
    }
    else if (sscanf(dgm, "PUB %31s", partido) == 1) {
        reenviar_a_suscriptores(partido, dgm, lg); // si el datagrama es de publicacion
    }
    else {
        printf("[broker UDP] Datagrama desconocido\n");
    }
}

/* Devuelve 1 si a y b son la misma IP y el mismo puerto. */
int misma_direccion(const struct sockaddr_in *a, const struct sockaddr_in *b)
{
    return (a->sin_addr.s_addr == b->sin_addr.s_addr) && (a->sin_port == b->sin_port);
}

/* Agrega (dir, partido) si no existe ya. */
void agregar_suscriptor(const struct sockaddr_in *dir, const char *partido)
{
    // verificacion duplicados
    for (int i = 0; i < n_suscripciones; i++) {
        if (misma_direccion(&suscripciones[i].dir, dir) &&
            strcmp(suscripciones[i].partido, partido) == 0) {
            return;
        }
    }

    // verificar que haya espacio
    if (n_suscripciones >= MAX_SUSCRIPCIONES) {
        fprintf(stderr, "[broker UDP] Error: tabla de suscripciones llena\n");
        return; // no hay espacion
    }

    // guardar nueva suscripcion
    suscripciones[n_suscripciones].dir = *dir;
    snprintf(suscripciones[n_suscripciones].partido, PARTIDO_MAX, "%s", partido);
    n_suscripciones++;

    // mostrar lista de suscriptores
    imprimir_suscriptores();
}

/* Reenvia el datagrama a cada direccion suscrita al partido. */
void reenviar_a_suscriptores(const char *partido, const char *dgm, size_t lg)
{
    int enviados = 0;
    for (int i = 0; i < n_suscripciones; i++) {
        if (strcmp(suscripciones[i].partido, partido) == 0) {
            ssize_t res = sendto(sock_broker, dgm, lg, 0,
                    (const struct sockaddr *)&suscripciones[i].dir, sizeof suscripciones[i].dir);
            if (res == -1) {
                perror("sendto");
            } else {
                enviados++;
            }
        }
    }

    printf("[broker UDP] PUB %s -> reenviado a %d suscriptor(es)\n", partido, enviados);
}

int main(int argc, char *argv[])
{
    if (argc != 2) {
        fprintf(stderr, "Uso: %s <puerto>\n", argv[0]);
        return 1;
    }
    int puerto = leer_puerto(argv[1]);
    setvbuf(stdout, NULL, _IOLBF, 0);

    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = al_recibir_senal;          /* sin SA_RESTART: recvfrom() retorna EINTR */
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    /* p.190: socket UDP. */
    sock_broker = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock_broker == -1) { perror("socket"); return 1; }

    /* p.192: bind al puerto conocido. */
    struct sockaddr_in dir;
    memset(&dir, 0, sizeof dir);
    dir.sin_family      = AF_INET;
    dir.sin_addr.s_addr = htonl(INADDR_ANY);
    dir.sin_port        = htons((uint16_t)puerto);
    if (bind(sock_broker, (struct sockaddr *)&dir, sizeof dir) == -1) {
        perror("bind");
        close(sock_broker);
        return 1;
    }
    printf("[broker UDP] escuchando en el puerto %d, proceso %d (Ctrl+C para salir)\n",
           puerto, (int)getpid());

    char dgm[DATAGRAMA_MAX + 1];
    while (seguir) {
        struct sockaddr_in origen;
        socklen_t lg = sizeof origen;

        /* p.208: un recvfrom entrega UN datagrama completo y la direccion del emisor. */
        ssize_t n = recvfrom(sock_broker, dgm, DATAGRAMA_MAX, 0, (struct sockaddr *)&origen, &lg);
        if (n == -1) {
            if (errno == EINTR) continue;         /* Ctrl+C: el while revisa "seguir" */
            perror("recvfrom");
            continue;
        }
        dgm[n] = '\0';

        char ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &origen.sin_addr, ip, sizeof ip);
        printf("[broker UDP] %zd bytes de %s:%u: %s%s", n, ip, ntohs(origen.sin_port), dgm,
               (n > 0 && dgm[n - 1] == '\n') ? "" : "\n");

        procesar_datagrama(dgm, (size_t)n, &origen);
    }

    printf("[broker UDP] apagando\n");
    imprimir_suscriptores();
    close(sock_broker);        /* p.206: en UDP no se envia nada a nadie al cerrar */
    printf("[broker UDP] detenido\n");
    return 0;
}
