/*
 * broker_udp.c  -  Laboratorio 3 (ISIS-2311L, Uniandes 2026-20)  -  ESQUELETO
 * Grupo: ____    Integrantes: ____________ (codigo) / ____________ / ____________
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
 * YA HECHO    socket y bind (p.190, p.192), bucle de recvfrom (p.208) con origen,
 *             cierre limpio con Ctrl+C, imprimir_suscriptores.
 * POR HACER   TODO 1 .. TODO 4.
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

void imprimir_suscriptores(void);                                                    /* hecho  */
void procesar_datagrama(char *dgm, size_t lg, const struct sockaddr_in *origen);    /* TODO 1 */
int  misma_direccion(const struct sockaddr_in *a, const struct sockaddr_in *b);     /* TODO 2 */
void agregar_suscriptor(const struct sockaddr_in *dir, const char *partido);        /* TODO 3 */
void reenviar_a_suscriptores(const char *partido, const char *dgm, size_t lg);      /* TODO 4 */

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
    /* TODO 1 - distinguir SUB y PUB:
     *  1. char partido[PARTIDO_MAX];
     *  2. Si sscanf(dgm, "SUB %31s", partido) == 1 -> agregar_suscriptor(origen, partido).
     *  3. Si sscanf(dgm, "PUB %31s", partido) == 1 -> reenviar_a_suscriptores(partido, dgm, lg).
     *  4. Otro caso: imprimir "[broker UDP] datagrama desconocido".
     *  No hace falta buscar '\n': en UDP un recvfrom = un datagrama = un mensaje (p.208).
     */
    (void)dgm;
    (void)lg;
    (void)origen;
}

/* Devuelve 1 si a y b son la misma IP y el mismo puerto. */
int misma_direccion(const struct sockaddr_in *a, const struct sockaddr_in *b)
{
    /* TODO 2 - comparar campo a campo (NO usar memcmp sobre toda la estructura: tiene relleno):
     *   return a->sin_addr.s_addr == b->sin_addr.s_addr && a->sin_port == b->sin_port;  */
    (void)a;
    (void)b;
    return 0;
}

/* Agrega (dir, partido) si no existe ya. */
void agregar_suscriptor(const struct sockaddr_in *dir, const char *partido)
{
    /* TODO 3 - tabla de suscripciones:
     *  1. Si ya hay una fila con misma_direccion(...) y el mismo partido, return sin duplicar.
     *     (el suscriptor puede reenviar su SUB porque en UDP tambien se puede perder)
     *  2. Si n_suscripciones == MAX_SUSCRIPCIONES, avisar y return.
     *  3. Copiar *dir y el partido en suscripciones[n_suscripciones]; n_suscripciones++.
     *  4. imprimir_suscriptores();
     *  Pregunta para el informe: si el suscriptor se va, ?como se entera el broker? (no hay FIN)
     */
    (void)dir;
    (void)partido;
}

/* Reenvia el datagrama a cada direccion suscrita al partido. */
void reenviar_a_suscriptores(const char *partido, const char *dgm, size_t lg)
{
    /* TODO 4 - reenvio por tema (fan-out):
     *  1. Recorrer la tabla; si el partido coincide:
     *       sendto(sock_broker, dgm, lg, 0, (struct sockaddr *)&suscripciones[i].dir,
     *              sizeof suscripciones[i].dir);          (p.207)
     *  2. Si sendto devuelve -1, perror("sendto") y seguir con los demas.
     *  3. Contar a cuantos se reenvio e imprimir "[broker UDP] PUB AvsB seq=3 -> 2 suscriptores".
     *  Observe: sendto casi nunca bloquea; si el suscriptor no alcanza a leer, sus
     *  datagramas se descartan en SU buffer de recepcion y nadie avisa.
     */
    (void)partido;
    (void)dgm;
    (void)lg;
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
